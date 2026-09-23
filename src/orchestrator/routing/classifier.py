"""Fixed, offline task taxonomy classifier used before node scheduling."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

from orchestrator.config.effective import ClassifierSpec, Complexity, Risk, TaskClass


_MAX_INPUT_CHARS = 100_000
_CLASS_CUES: tuple[tuple[TaskClass, tuple[str, ...]], ...] = (
    ("mechanical", ("formatting", "lint only", "schema conversion", "格式化", "结构化提取", "schema转换", "枚举提取")),
    ("document_analysis", ("document analysis", "summarize the document", "summarize pdf", "分析文档", "总结文档", "pdf摘要")),
    ("code_change", ("implement", "refactor", "fix the bug", "modify code", "write code", "实现", "补齐", "修改代码", "修复代码", "重构")),
    ("research", ("research", "look up sources", "web search", "查资料", "调研", "搜索资料", "查找来源")),
    ("review", ("code review", "review this", "audit", "inspect for", "审查", "审核", "审计", "检查项目", "检查代码")),
    ("planning", ("make a plan", "plan the", "planning", "roadmap", "制定计划", "规划", "计划")),
    ("analysis", ("explain", "diagnose", "debug", "analyze", "分析", "解释", "诊断", "排查")),
)
_HIGH_COMPLEXITY = (
    "concurrency", "race condition", "distributed", "architecture", "multi-system",
    "security boundary", "并发", "竞态", "分布式", "架构", "跨系统", "安全边界", "多阶段",
)
_LOW_COMPLEXITY = (
    "simple", "small change", "single-step", "formatting", "just rename",
    "简单", "小改", "单步", "格式化", "只改名称",
)
_CRITICAL_RISK = (
    "production deploy", "publish to production", "irreversible delete", "leak credentials",
    "expose secret", "financial transfer", "生产发布", "不可恢复删除", "泄露密钥", "资金转移",
)
_HIGH_RISK = (
    "security", "permission", "credential", "secret", "external mutation", "publish", "push",
    "deploy", "delete", "system install", "approval", "安全", "权限", "密钥", "凭据",
    "外部写入", "发布", "推送", "部署", "删除", "安装系统", "审批",
)


class ClassificationResult(BaseModel):
    """Reproducible labels and opaque input identity; never stores task text."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    classifier_id: Literal["orchestrator.deterministic.v1"]
    classifier_version: StrictStr = Field(min_length=1)
    normalization_version: StrictStr = Field(min_length=1)
    taxonomy_version: StrictStr = Field(min_length=1)
    task_class: TaskClass
    complexity: Complexity
    risk: Risk
    confidence: StrictInt = Field(ge=0, le=100)
    input_hash: StrictStr = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evidence_codes: tuple[StrictStr, ...]

    @field_validator("evidence_codes", mode="before")
    @classmethod
    def freeze_evidence(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, (tuple, list)):
            raise ValueError("evidence_codes must be an array")
        return tuple(value)

    @property
    def content_hash(self) -> str:
        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class TaskClassifier:
    """Version-pinned, keyword-rule classifier with fail-closed uncertainty."""

    def __init__(self, specification: ClassifierSpec) -> None:
        if specification.id != "orchestrator.deterministic.v1":
            raise ValueError("only the built-in deterministic classifier is supported")
        if specification.normalization_version != "unicode-nfkc-v1":
            raise ValueError("unsupported classifier normalization version")
        if specification.taxonomy_version != "maestro-task-taxonomy-v1":
            raise ValueError("unsupported task taxonomy version")
        self.specification = specification

    def classify(self, text: str) -> ClassificationResult:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("task text must be a non-blank string")
        if len(text) > _MAX_INPUT_CHARS:
            raise ValueError("task text exceeds the classifier input limit")
        normalized = " ".join(unicodedata.normalize("NFKC", text).casefold().split())
        matched = tuple(
            (task_class, cue)
            for task_class, cues in _CLASS_CUES
            for cue in cues
            if cue in normalized
        )
        classes = {task_class for task_class, _ in matched}
        if len(classes) == 1:
            task_class = next(iter(classes))
            confidence = 100
            class_evidence = tuple(
                f"class:{task_class}:{index}"
                for index, _ in enumerate(sorted(cue for label, cue in matched if label == task_class), start=1)
            )
        else:
            task_class = "unknown"
            confidence = 0
            class_evidence = ("classification:ambiguous" if classes else "classification:no_match",)

        high_complexity = _matches_any(normalized, _HIGH_COMPLEXITY)
        low_complexity = _matches_any(normalized, _LOW_COMPLEXITY)
        if task_class == "unknown":
            complexity: Complexity = "unknown"
        elif high_complexity:
            complexity = "high"
        elif low_complexity or (task_class == "mechanical" and len(normalized) <= 400):
            complexity = "low"
        else:
            complexity = "medium"

        critical = _matches_any(normalized, _CRITICAL_RISK)
        high_risk = _matches_any(normalized, _HIGH_RISK)
        if task_class == "unknown":
            risk: Risk = "high"
        elif critical:
            risk = "critical"
        elif high_risk:
            risk = "high"
        else:
            risk = "low"

        evidence = [*class_evidence]
        if high_complexity:
            evidence.append("complexity:high_cue")
        elif low_complexity:
            evidence.append("complexity:low_cue")
        if critical:
            evidence.append("risk:critical_cue")
        elif high_risk:
            evidence.append("risk:high_cue")
        if task_class == "unknown":
            evidence.extend(("guard:minimum_standard_tier", "guard:independent_review"))

        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return ClassificationResult(
            classifier_id=self.specification.id,
            classifier_version=self.specification.version,
            normalization_version=self.specification.normalization_version,
            taxonomy_version=self.specification.taxonomy_version,
            task_class=task_class,
            complexity=complexity,
            risk=risk,
            confidence=confidence,
            input_hash=f"sha256:{digest}",
            evidence_codes=tuple(evidence),
        )


def _matches_any(text: str, cues: tuple[str, ...]) -> bool:
    return any(cue in text for cue in cues)


__all__ = ["ClassificationResult", "TaskClassifier"]

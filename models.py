from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class RunInput:
    config_path: str
    dry_run: bool = True

@dataclass
class EnrichInput:
    name: str
    dry_run: bool = False

@dataclass
class ReindexInput:
    command: Dict[str, Any]
    dry_run: bool = False

@dataclass
class TaskInput:
    task_id: str

@dataclass
class ValidationInput:
    command: Dict[str, Any]
    task_result: Dict[str, Any] = field(default_factory=dict)

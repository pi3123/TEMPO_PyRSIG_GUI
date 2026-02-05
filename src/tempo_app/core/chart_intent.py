"""
Chart Intent - Structured specification for AI-generated charts.

Supports:
- Single or multiple Y columns for comparison
- Computed expressions (e.g., "NO2_TropVCD / HCHO_TropVCD")
- Temporal aggregation (hour, date, month, etc.)
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum
import json
import logging
import re


class ChartType(Enum):
    """Supported chart types."""
    LINE = "line"
    BAR = "bar"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"


class Aggregation(Enum):
    """Supported aggregation methods."""
    MEAN = "mean"
    SUM = "sum"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"


@dataclass
class ChartIntent:
    """
    Structured chart specification from AI.
    
    Supports:
    - Single or multiple Y columns
    - Computed expressions (e.g., NO2 / HCHO)
    - Group by site for multi-site comparison
    
    Example JSON inputs:
        Single column:
        {"chart_type": "line", "x": "hour", "y": ["NO2_TropVCD"]}
        
        Multiple columns:
        {"chart_type": "line", "x": "hour", "y": ["NO2_TropVCD", "HCHO_TropVCD"]}
        
        Group by site:
        {"chart_type": "line", "x": "hour", "y": ["NO2_TropVCD"], "group_by": "site_code"}
    """
    chart_type: ChartType
    x_column: str                      # Column for x-axis (or "hour", "date", etc.)
    y_columns: list[str]               # List of columns or expressions for y-axis
    aggregation: Aggregation           # How to aggregate values
    group_by: Optional[str] = None     # Column to group by (e.g., "site_code")
    filters: dict = field(default_factory=dict)
    title: Optional[str] = None
    
    # Special x-axis values that don't need to be actual columns
    SPECIAL_X_VALUES = {"hour", "date", "month", "day_of_week", "year"}
    
    # Allowed operators in expressions
    ALLOWED_OPERATORS = {'+', '-', '*', '/', '(', ')', ' '}
    
    @classmethod
    def from_json(cls, data: dict) -> "ChartIntent":
        """Parse AI JSON output into ChartIntent."""
        try:
            chart_type = ChartType(data.get("chart_type", "line"))
        except ValueError:
            logging.warning(f"Unknown chart_type, defaulting to line")
            chart_type = ChartType.LINE
            
        try:
            aggregation = Aggregation(data.get("aggregation", "mean"))
        except ValueError:
            logging.warning(f"Unknown aggregation, defaulting to mean")
            aggregation = Aggregation.MEAN
        
        # Handle both single string and list for y
        y_value = data.get("y", [])
        if isinstance(y_value, str):
            y_columns = [y_value]
        elif isinstance(y_value, list):
            y_columns = y_value if y_value else [""]
        else:
            y_columns = [str(y_value)]
        
        return cls(
            chart_type=chart_type,
            x_column=data.get("x", "date"),
            y_columns=y_columns,
            aggregation=aggregation,
            group_by=data.get("group_by"),
            filters=data.get("filters", {}),
            title=data.get("title"),
        )
    
    def is_expression(self, y_col: str) -> bool:
        """Check if a y column is an expression (contains operators)."""
        return any(op in y_col for op in ['+', '-', '*', '/'])
    
    def get_expression_columns(self, y_col: str) -> list[str]:
        """Extract column names from an expression like 'NO2 / HCHO'."""
        if not self.is_expression(y_col):
            return [y_col]
        
        # Split by operators and extract column names
        parts = re.split(r'[\+\-\*\/\(\)\s]+', y_col)
        return [p.strip() for p in parts if p.strip() and not p.strip().replace('.', '').isdigit()]
    
    def validate(self, schema: dict) -> list[str]:
        """Validate intent against dataset schema."""
        errors = []
        available_cols = schema.get("columns", [])
        
        # Check each y column/expression
        if not self.y_columns:
            errors.append("At least one Y-axis column is required")
        
        for y_col in self.y_columns:
            if self.is_expression(y_col):
                # Validate columns in expression
                expr_cols = self.get_expression_columns(y_col)
                for col in expr_cols:
                    if col not in available_cols:
                        errors.append(f"Column '{col}' in expression not found")
            else:
                if y_col and y_col not in available_cols:
                    errors.append(f"Column '{y_col}' not found. Available: {available_cols[:5]}...")
        
        # Check x_column
        if self.x_column not in available_cols and self.x_column not in self.SPECIAL_X_VALUES:
            errors.append(f"X-axis '{self.x_column}' not found")
        
        # Check filter columns
        for col in self.filters.keys():
            if col not in available_cols:
                errors.append(f"Filter column '{col}' not found")
                
        return errors
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging/serialization."""
        return {
            "chart_type": self.chart_type.value,
            "x": self.x_column,
            "y": self.y_columns,
            "aggregation": self.aggregation.value,
            "filters": self.filters,
            "title": self.title,
        }
    
    def __str__(self) -> str:
        """Human-readable representation."""
        y_str = ", ".join(self.y_columns)
        parts = [f"{self.chart_type.value} chart", f"x={self.x_column}", f"y=[{y_str}]"]
        if self.filters:
            parts.append(f"filters={self.filters}")
        return ", ".join(parts)


class ChartIntentError(Exception):
    """Raised when chart intent parsing or validation fails."""
    pass


def parse_intent_from_response(response_text: str, schema: dict) -> ChartIntent:
    """Parse AI response text into a validated ChartIntent."""
    json_str = _extract_json(response_text)
    
    if not json_str:
        raise ChartIntentError(
            f"Could not find JSON in AI response. Response was: {response_text[:200]}"
        )
    
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ChartIntentError(f"Invalid JSON in response: {e}")
    
    intent = ChartIntent.from_json(data)
    
    # Validate against schema
    errors = intent.validate(schema)
    if errors:
        raise ChartIntentError(f"Invalid intent: {'; '.join(errors)}")
    
    logging.info(f"[CHART_INTENT] Parsed: {intent}")
    return intent


def _extract_json(text: str) -> str:
    """Extract JSON object from text that may contain other content."""
    text = text.strip()
    
    if "```json" in text:
        start = text.find("```json") + len("```json")
        end = text.find("```", start)
        if end > start:
            return text[start:end].strip()
    
    if "```" in text:
        start = text.find("```") + len("```")
        end = text.find("```", start)
        if end > start:
            return text[start:end].strip()
    
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        return text[start:end]
    
    return ""

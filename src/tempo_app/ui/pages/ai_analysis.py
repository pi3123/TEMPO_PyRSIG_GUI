"""
AI Analysis page - Natural language chart generation interface.

Users can:
1. Select a dataset
2. Type natural language queries
3. View generated code (and edit it)
4. See the resulting plot
5. Save and manage analyses
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
import logging
import traceback

import flet as ft
import pandas as pd

from ..theme import Colors, Spacing, card_style
from ..components.widgets import SectionCard
from ...core.config import ConfigManager
from ...core.chart_generator import ChartGenerator, ChartGenerationError
from ...core.chart_executor import ChartExecutor, ChartExecutionError
from ...core.df_converter import DataFrameConverter
from ...storage.database import Database
from ...storage.models import Analysis, Dataset


class AIAnalysisPage(ft.Container):
    """
    AI-powered chart generation page.

    Layout:
    ┌─────────────────────────────────────────────┐
    │ Dataset: [Select Dataset ▾]                │
    ├─────────────────────────────────────────────┤
    │ Query: [Type your question here...       ] │
    │        [Generate Chart]                     │
    ├─────────────────────────────────────────────┤
    │ Generated Code:              [Edit] [Run]   │
    │ ┌─────────────────────────────────────────┐ │
    │ │ import matplotlib.pyplot as plt         │ │
    │ │ ...                                     │ │
    │ └─────────────────────────────────────────┘ │
    ├─────────────────────────────────────────────┤
    │ Result:                                     │
    │ [Chart displayed here]                      │
    ├─────────────────────────────────────────────┤
    │ Saved Analyses:                             │
    │ • NO₂ vs HCHO (2 min ago)                  │
    │ • Hourly trends (5 min ago)                │
    └─────────────────────────────────────────────┘
    """

    def __init__(self, db: Database, data_dir: Path):
        super().__init__()
        self.db = db
        self.data_dir = data_dir
        self.config = ConfigManager()
        
        # State - use a private dict to avoid Flet's property tracking
        # which causes "truth value of DataFrame is ambiguous" errors
        self._state = {
            "selected_dataset": None,
            "current_df": None,
            "current_analysis": None,
            "generator": None,
        }
        
        self._build()

    def did_mount(self):
        """Called when control is added to page - load data async."""
        self.page.run_task(self._load_datasets_async)
        # Load models
        api_key = self.config.get("gemini_api_key", "")
        if api_key:
            self.page.run_task(self._load_models_async)

    async def _load_datasets_async(self):
        """Load all datasets into dropdown."""
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        
        # Filter to only complete datasets
        complete_datasets = [ds for ds in datasets if ds.is_complete]
        
        # Populate dropdown
        options = []
        for ds in complete_datasets:
            options.append(ft.DropdownOption(key=ds.id, text=ds.name))
        self._dataset_dropdown.options = options
        
        if options:
            self._dataset_dropdown.value = options[0].key
            # Auto-load the first dataset
            await self._load_selected_dataset_async()
        
        self.update()

    async def _load_models_async(self):
        """Load available Gemini models into dropdown."""
        try:
            api_key = self.config.get("gemini_api_key", "")
            if not api_key:
                return

            models = await asyncio.to_thread(
                ChartGenerator.list_available_models,
                api_key
            )
            
            if not models:
                return

            current_model = self.config.get("gemini_model", "gemini-2.0-flash-lite")
            
            # Populate dropdown
            options = []
            model_names = []
            for m in models:
                options.append(ft.DropdownOption(key=m['name'], text=m['display_name']))
                model_names.append(m['name'])
            
            self._model_dropdown.options = options
            
            # Set value
            if current_model in model_names:
                self._model_dropdown.value = current_model
            elif model_names:
                self._model_dropdown.value = models[0]['name']
                # Don't auto-save to config on load, just UI default
            
            self._model_dropdown.visible = True
            self._save_model_btn.visible = True
            self.update()
            
        except Exception as ex:
            logging.error(f"Error loading models: {ex}")

    def _build(self):
        """Build the page UI."""
        
        # Header
        header = ft.Column([
            ft.Row([
                ft.Icon(ft.Icons.AUTO_AWESOME, size=32, color=Colors.PRIMARY),
                ft.Text("AI Chart Analysis", size=28, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
            ], spacing=12),
            ft.Text(
                "Generate charts from natural language queries using Google Gemini AI",
                size=14, color=Colors.ON_SURFACE_VARIANT
            ),
            ft.Divider(height=20, color=Colors.DIVIDER),
        ])

        # Model selector (initially hidden until loaded)
        self._model_dropdown = ft.Dropdown(
            label="AI Model",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=250,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            visible=False,
        )
        
        self._save_model_btn = ft.IconButton(
            icon=ft.Icons.SAVE,
            icon_color=Colors.PRIMARY,
            tooltip="Save Model Selection",
            on_click=self._on_save_model,
            visible=False,
        )

        # Dataset selector
        self._dataset_dropdown = ft.Dropdown(
            label="Select Dataset",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=400,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
        )
        self._dataset_dropdown.on_change = self._on_dataset_selected

        # Query input
        self._query_field = ft.TextField(
            label="Natural Language Query",
            hint_text="e.g., Plot NO2 trends by hour at BV site",
            multiline=False,
            expand=True,
            border_color=Colors.BORDER,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            on_submit=self._on_generate_clicked,
        )

        self._generate_btn = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.AUTO_AWESOME, size=18),
                ft.Text("Generate Chart", color=Colors.ON_PRIMARY),
            ], spacing=6, tight=True),
            on_click=self._on_generate_clicked,
        )

        # Code editor - now shows AI interpretation, not editable code
        self._code_editor = ft.TextField(
            label="AI Interpretation",
            multiline=True,
            min_lines=4,
            max_lines=8,
            expand=True,
            border_color=Colors.BORDER,
            text_style=ft.TextStyle(
                font_family="Consolas, monospace",
                size=12,
                color=Colors.ON_SURFACE,
            ),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            read_only=True,  # No longer editable
        )

        self._run_btn = ft.TextButton(
            content=ft.Row([
                ft.Icon(ft.Icons.PLAY_ARROW, size=18, color=Colors.PRIMARY),
                ft.Text("Run Code", color=Colors.PRIMARY),
            ], spacing=4, tight=True),
            on_click=self._on_run_code_clicked,
        )

        self._save_btn = ft.TextButton(
            content=ft.Row([
                ft.Icon(ft.Icons.SAVE, size=18, color=Colors.PRIMARY),
                ft.Text("Save Analysis", color=Colors.PRIMARY),
            ], spacing=4, tight=True),
            on_click=self._on_save_clicked,
        )

        # Plot viewer
        self._plot_image = ft.Image(
            src="",
            visible=False,
        )

        self._plot_placeholder = ft.Container(
            content=ft.Column([
                ft.Icon(ft.Icons.INSERT_CHART, size=64, color=Colors.ON_SURFACE_VARIANT),
                ft.Text("Chart will appear here", color=Colors.ON_SURFACE_VARIANT),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=16),
            alignment=ft.Alignment(0, 0),
            height=400,
            bgcolor=Colors.SURFACE_VARIANT,
            border_radius=8,
        )

        # Status indicator
        self._status_text = ft.Text(
            "",
            size=12,
            color=Colors.INFO,
            italic=True,
        )

        self._progress_bar = ft.ProgressBar(visible=False, color=Colors.PRIMARY)

        # Saved analyses history
        self._history_list = ft.Column(
            spacing=4,
            scroll=ft.ScrollMode.AUTO,
        )

        # Layout sections
        dataset_section = ft.Container(
            content=ft.Row([
                self._dataset_dropdown,
                ft.Container(width=20), # Spacer
                ft.Row([
                    self._model_dropdown,
                    self._save_model_btn
                ], visible=True)
            ]),
            padding=Spacing.MD,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )

        query_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    self._query_field,
                    self._generate_btn,
                ], spacing=8),
                # Example queries
                ft.Container(
                    content=ft.Row([
                        ft.Text("Try: ", size=12, color=Colors.ON_SURFACE_VARIANT, italic=True),
                        ft.TextButton("NO2 by hour", on_click=lambda e: self._set_query("NO2 by hour"), style=ft.ButtonStyle(padding=2)),
                        ft.Text("•", color=Colors.ON_SURFACE_VARIANT),
                        ft.TextButton("Compare NO2 and HCHO", on_click=lambda e: self._set_query("Compare NO2 and HCHO by hour"), style=ft.ButtonStyle(padding=2)),
                        ft.Text("•", color=Colors.ON_SURFACE_VARIANT),
                        ft.TextButton("NO2/HCHO ratio", on_click=lambda e: self._set_query("NO2/HCHO ratio by hour"), style=ft.ButtonStyle(padding=2)),
                        ft.Text("•", color=Colors.ON_SURFACE_VARIANT),
                        ft.TextButton("Daily trends", on_click=lambda e: self._set_query("NO2 daily trends"), style=ft.ButtonStyle(padding=2)),
                    ], spacing=4, wrap=True),
                    padding=ft.padding.only(left=4, top=4),
                ),
                self._status_text,
                self._progress_bar,
            ], spacing=8),
            padding=Spacing.MD,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )

        code_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("AI Interpretation", weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                    ft.Container(expand=True),
                    self._save_btn,
                ]),
                self._code_editor,
            ], spacing=8),
            padding=Spacing.MD,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )

        result_section = ft.Container(
            content=ft.Column([
                ft.Text("Result", weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                ft.Stack([
                    self._plot_placeholder,
                    self._plot_image,
                ]),
            ], spacing=8),
            padding=Spacing.MD,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )

        history_section = ft.Container(
            content=ft.Column([
                ft.Text("Saved Analyses", weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                ft.Container(
                    content=self._history_list,
                    height=150,
                    border=ft.border.all(1, Colors.BORDER),
                    border_radius=4,
                    padding=4,
                ),
            ], spacing=8),
            padding=Spacing.MD,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )

        # Main layout
        self.content = ft.Column([
            header,
            dataset_section,
            ft.Container(height=8),
            query_section,
            ft.Container(height=8),
            code_section,
            ft.Container(height=8),
            result_section,
            ft.Container(height=8),
            history_section,
            ft.Container(height=20),
        ], scroll=ft.ScrollMode.AUTO, expand=True)
        
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    def _on_dataset_selected(self, e):
        """Handle dataset selection."""
        self.page.run_task(self._load_selected_dataset_async)

    async def _load_selected_dataset_async(self):
        """Load the selected dataset - LAZY: only loads schema, not full data."""
        dataset_id = self._dataset_dropdown.value
        logging.info(f"[AI_ANALYSIS] Starting dataset load, ID: {dataset_id}")
        if not dataset_id:
            logging.warning("[AI_ANALYSIS] No dataset_id, returning early")
            return

        self._status_text.value = "Loading dataset metadata..."
        self._status_text.color = Colors.INFO
        self.update()

        try:
            # Step 1: Get dataset from DB
            self._state["selected_dataset"] = await asyncio.to_thread(
                self.db.get_dataset, dataset_id
            )

            if not self._state["selected_dataset"] or not self._state["selected_dataset"].file_path:
                self._status_text.value = "Dataset not found or incomplete"
                self._status_text.color = Colors.ERROR
                self.update()
                return

            # Step 2: Check file exists
            dataset_path = Path(self._state["selected_dataset"].file_path)
            if not dataset_path.exists():
                self._status_text.value = "Dataset file not found"
                self._status_text.color = Colors.ERROR
                self.update()
                return
            
            # Store path for later lazy loading
            self._state["dataset_path"] = dataset_path

            # Step 3: Load ONLY schema (fast - doesn't load full data)
            logging.info("[AI_ANALYSIS] Extracting schema (lazy load)...")
            schema = await asyncio.to_thread(
                DataFrameConverter.get_schema_fast,
                dataset_path
            )
            self._state["schema"] = schema
            logging.info(f"[AI_ANALYSIS] Schema loaded: {len(schema.get('columns', []))} columns")
            
            # Step 4: Add site column hint to schema if sites exist
            sites = await asyncio.to_thread(self.db.get_all_sites)
            if sites and len(sites) > 0:
                site_codes = [s.code for s in sites[:10]]  # Sample
                schema["columns"].extend(["site_code", "site_name"])
                schema["dtypes"]["site_code"] = "object"
                schema["dtypes"]["site_name"] = "object"
                schema["sample_values"]["site_code"] = site_codes
                self._state["sites"] = sites
            
            # Clear current_df to ensure fresh load on execute
            self._state["current_df"] = None
            
            # Success - show schema info
            col_count = len(schema.get("columns", []))
            shape = schema.get("shape", (0, 0))
            self._status_text.value = f"Ready: {col_count} columns, ~{shape[0]:,} data points"
            self._status_text.color = Colors.SUCCESS

            # Load analysis history
            await self._refresh_history_async()

        except Exception as ex:
            logging.error(f"Error loading dataset: {ex}")
            logging.error(f"Full traceback:\n{traceback.format_exc()}")
            self._status_text.value = f"Error loading dataset: {str(ex)}"
            self._status_text.color = Colors.ERROR

        self.update()

    def _set_query(self, query: str):
        """Set the query field and trigger generation."""
        self._query_field.value = query
        self.update()

    def _on_generate_clicked(self, e):
        """Handle 'Generate Chart' button click."""
        self.page.run_task(self._generate_chart_async)

    async def _generate_chart_async(self):
        """Generate chart using intent-based AI workflow.
        
        New workflow:
        1. AI generates JSON intent (not code)
        2. ChartExecutor uses xarray to aggregate lazily
        3. Result: Memory-efficient, reliable chart generation
        """
        if not self._state["selected_dataset"] or not self._state.get("schema"):
            self._status_text.value = "Please select a dataset first"
            self._status_text.color = Colors.ERROR
            self.update()
            return

        query = self._query_field.value.strip() if self._query_field.value else ""
        if not query:
            self._status_text.value = "Please enter a query"
            self._status_text.color = Colors.ERROR
            self.update()
            return

        self._status_text.value = "AI is interpreting your query..."
        self._status_text.color = Colors.INFO
        self._progress_bar.visible = True
        self.update()

        try:
            # Lazy init generator
            if self._state["generator"] is None:
                self._state["generator"] = ChartGenerator()

            # Step 1: Get JSON intent from AI (no code generation!)
            logging.info(f"[AI_ANALYSIS] Generating intent for: {query}")
            intent = await asyncio.to_thread(
                self._state["generator"].generate_intent,
                query,
                self._state["schema"]
            )

            # Display intent (for transparency)
            y_str = ", ".join(intent.y_columns)
            intent_str = f"Chart: {intent.chart_type.value}\nX: {intent.x_column}\nY: {y_str}\nAggregation: {intent.aggregation.value}"
            if intent.group_by:
                intent_str += f"\nGroup by: {intent.group_by}"
            if intent.filters:
                intent_str += f"\nFilters: {intent.filters}"
            self._code_editor.value = intent_str
            
            self._status_text.value = f"Generating {intent.chart_type.value} chart..."
            self.update()

            # Step 2: Execute intent with xarray (memory efficient!)
            logging.info(f"[AI_ANALYSIS] Executing intent: {intent}")
            executor = ChartExecutor()
            
            output_dir = self.data_dir / "analyses"
            output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = output_dir / f"plot_{timestamp}.png"
            
            plot_path = await asyncio.to_thread(
                executor.execute,
                intent,
                self._state["dataset_path"],
                output_path
            )

            # Step 3: Display result
            self._plot_image.src = str(plot_path)
            self._plot_image.visible = True
            self._plot_placeholder.visible = False

            self._status_text.value = "Chart generated successfully!"
            self._status_text.color = Colors.SUCCESS

            # Create analysis object (for saving)
            self._state["current_analysis"] = Analysis.new(
                dataset_id=self._state["selected_dataset"].id,
                query=query,
                code=intent_str,  # Store intent description
                plot_path=str(plot_path)
            )

        except (ChartGenerationError, ChartExecutionError) as ex:
            self._status_text.value = f"Failed: {str(ex)}"
            self._status_text.color = Colors.ERROR
        except Exception as ex:
            logging.error(f"Unexpected error: {ex}\n{traceback.format_exc()}")
            self._status_text.value = f"Unexpected error: {str(ex)}"
            self._status_text.color = Colors.ERROR
        finally:
            self._progress_bar.visible = False
            self.update()

    def _on_run_code_clicked(self, e):
        """Handle 'Run Code' button (for edited code)."""
        self.page.run_task(self._run_edited_code_async)

    async def _run_edited_code_async(self):
        """Run the edited code."""
        code = self._code_editor.value.strip() if self._code_editor.value else ""
        if not code:
            self._status_text.value = "No code to execute"
            self._status_text.color = Colors.ERROR
            self.update()
            return

        query = self._query_field.value.strip() if self._query_field.value else "Custom plot"
        await self._execute_code_async(code, query)

    async def _execute_code_async(self, code: str, query: str):
        """Execute the code and display the plot - loads DataFrame here (lazy)."""
        
        # Lazy load DataFrame if not already loaded
        if self._state["current_df"] is None:
            if not self._state.get("dataset_path"):
                self._status_text.value = "No dataset selected"
                self._status_text.color = Colors.ERROR
                self.update()
                return
            
            self._status_text.value = "Loading data for execution..."
            self._status_text.color = Colors.INFO
            self.update()
            
            # Load DataFrame with downsampling to prevent memory explosion
            # AI analysis doesn't need millions of rows - 100k is enough for any chart
            MAX_ROWS_FOR_AI = 100_000
            
            df = await asyncio.to_thread(
                DataFrameConverter.dataset_to_dataframe,
                self._state["dataset_path"],
                include_coords=True,
                downsample=MAX_ROWS_FOR_AI  # Limit to 100k rows max
            )
            
            if df is None or df.empty:
                self._status_text.value = "Dataset is empty"
                self._status_text.color = Colors.WARNING
                self.update()
                return
            
            logging.info(f"[AI_ANALYSIS] Loaded {len(df)} rows for analysis (max {MAX_ROWS_FOR_AI})")
            
            # Add temporal features
            df = DataFrameConverter.add_temporal_features(df)
            
            # Add site data if available
            if self._state.get("sites"):
                site_data = [(s.code, s.name, s.latitude, s.longitude) for s in self._state["sites"]]
                df = DataFrameConverter.add_site_data(df, site_data)
            
            self._state["current_df"] = df

        self._status_text.value = "Executing code..."
        self._status_text.color = Colors.INFO
        self._progress_bar.visible = True
        self.update()

        try:
            # Lazy init generator
            if self._state["generator"] is None:
                self._state["generator"] = ChartGenerator()

            # Create output path
            output_dir = self.data_dir / "analyses"
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = output_dir / f"plot_{timestamp}.png"

            # Execute code
            plot_path = await asyncio.to_thread(
                self._state["generator"].execute_code,
                code,
                self._state["current_df"],
                output_path
            )

            # Display plot
            self._plot_image.src = str(plot_path)
            self._plot_image.visible = True
            self._plot_placeholder.visible = False

            self._status_text.value = "Chart generated successfully!"
            self._status_text.color = Colors.SUCCESS

            # Create analysis object (not saved yet)
            self._state["current_analysis"] = Analysis.new(
                dataset_id=self._state["selected_dataset"].id,
                query=query,
                code=code,
                plot_path=str(plot_path)
            )

        except ChartGenerationError as ex:
            self._status_text.value = f"Execution failed: {str(ex)}"
            self._status_text.color = Colors.ERROR
        except Exception as ex:
            logging.error(f"Unexpected error: {ex}")
            self._status_text.value = f"Unexpected error: {str(ex)}"
            self._status_text.color = Colors.ERROR
        finally:
            self._progress_bar.visible = False
            self.update()

    def _on_save_clicked(self, e):
        """Save the current analysis to database."""
        self.page.run_task(self._save_analysis_async)

    async def _save_analysis_async(self):
        """Save analysis to database."""
        if not self._state["current_analysis"]:
            self._status_text.value = "No analysis to save"
            self._status_text.color = Colors.ERROR
            self.update()
            return

        try:
            await asyncio.to_thread(
                self.db.save_analysis, self._state["current_analysis"]
            )
            self._status_text.value = "Analysis saved!"
            self._status_text.color = Colors.SUCCESS

            # Refresh history
            await self._refresh_history_async()

        except Exception as ex:
            logging.error(f"Save failed: {ex}")
            self._status_text.value = f"Save failed: {str(ex)}"
            self._status_text.color = Colors.ERROR

        self.update()

    async def _refresh_history_async(self):
        """Load saved analyses for current dataset."""
        if not self._state["selected_dataset"]:
            return

        analyses = await asyncio.to_thread(
            self.db.get_analyses_for_dataset, self._state["selected_dataset"].id
        )

        self._history_list.controls.clear()

        for analysis in analyses:
            # Calculate time ago
            time_ago = self._format_time_ago(analysis.created_at)

            item = ft.Container(
                content=ft.Row([
                    ft.Icon(ft.Icons.INSERT_CHART, size=18, color=Colors.PRIMARY),
                    ft.Column([
                        ft.Text(analysis.name, size=13, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                        ft.Text(
                            f"{analysis.query[:40]}{'...' if len(analysis.query) > 40 else ''} • {time_ago}",
                            size=11, color=Colors.ON_SURFACE_VARIANT
                        ),
                    ], spacing=2, expand=True),
                ], spacing=8),
                on_click=lambda e, a=analysis: self._load_analysis(a),
                padding=8,
                border_radius=4,
                bgcolor=Colors.SURFACE_VARIANT,
                ink=True,
            )
            self._history_list.controls.append(item)

    def _load_analysis(self, analysis: Analysis):
        """Load a saved analysis into the editor."""
        self._state["current_analysis"] = analysis
        self._query_field.value = analysis.query
        self._code_editor.value = analysis.code
        
        if Path(analysis.plot_path).exists():
            self._plot_image.src = analysis.plot_path
            self._plot_image.visible = True
            self._plot_placeholder.visible = False
        
        self._status_text.value = f"Loaded: {analysis.name}"
        self._status_text.color = Colors.INFO
        self.update()

    @staticmethod
    def _format_time_ago(dt: datetime) -> str:
        """Format datetime as 'X mins ago'."""
        delta = datetime.now() - dt
        seconds = delta.total_seconds()

        if seconds < 60:
            return "Just now"
        elif seconds < 3600:
            mins = int(seconds // 60)
            return f"{mins} min{'s' if mins != 1 else ''} ago"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        else:
            return dt.strftime("%Y-%m-%d")

    def _on_save_model(self, e):
        """Handle save model button click."""
        model_name = self._model_dropdown.value
        if model_name:
            self.config.set("gemini_model", model_name)
            
            # Reset generator to ensure new model is used
            self._state["generator"] = None
            
            if self.page:
                self.page.snack_bar = ft.SnackBar(
                    ft.Text(f"Model set to: {model_name}"),
                    duration=2000
                )
                self.page.snack_bar.open = True
                self.page.update()

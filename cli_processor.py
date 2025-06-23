from textual.app import App, ComposeResult
from textual.containers import Center, Middle, Vertical, Horizontal
from textual.widgets import Button, ProgressBar, Select, Header, Footer, Label, ListView, ListItem, DataTable, Input, DirectoryTree
from textual.screen import Screen, ModalScreen
from textual.message import Message
from rich.console import Console
from rich.panel import Panel
import time
from typing import List, Dict, Any, Optional, TypeVar, Generic, cast, TYPE_CHECKING, Protocol
from src.data_quality.core.framework import DataQualityFramework
from pyspark.sql import SparkSession
import os
from pathlib import Path

# ASCII Art Logo
LOGO = """
████████╗███████╗ ██████╗  █ ███████╗    ██████╗  █████╗ ████████╗ █████╗                                                                    
╚══██╔══╝██╔════╝██╔═══██╗ █ ██╔════╝    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗                                                                   
   ██║   █████╗  ██║   ██║   ███████╗    ██║  ██║███████║   ██║   ███████║                                                                   
   ██║   ██╔══╝  ██║   ██║   ╚════██║    ██║  ██║██╔══██║   ██║   ██╔══██║                                                                   
   ██║   ███████╗╚██████╔╝   ███████║    ██████╔╝██║  ██║   ██║   ██║  ██║                                                                   
   ╚═╝   ╚══════╝ ╚═════╝    ╚══════╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝                                                                   
                                                                                                                                          
██████╗ ██████╗  ██████╗  ██████╗███████╗███████╗███████╗██╗███╗   ██╗ ██████╗     ████████╗ ██████╗  ██████╗ ██╗     ██╗  ██╗██╗████████╗
██╔══██╗██╔══██╗██╔═══██╗██╔════╝██╔════╝██╔════╝██╔════╝██║████╗  ██║██╔════╝     ╚══██╔══╝██╔═══██╗██╔═══██╗██║     ██║ ██╔╝██║╚══██╔══╝
██████╔╝██████╔╝██║   ██║██║     █████╗  ███████╗███████╗██║██╔██╗ ██║██║  ███╗       ██║   ██║   ██║██║   ██║██║     █████╔╝ ██║   ██║   
██╔═══╝ ██╔══██╗██║   ██║██║     ██╔══╝  ╚════██║╚════██║██║██║╚██╗██║██║   ██║       ██║   ██║   ██║██║   ██║██║     ██╔═██╗ ██║   ██║   
██║     ██║  ██║╚██████╔╝╚██████╗███████╗███████║███████║██║██║ ╚████║╚██████╔╝       ██║   ╚██████╔╝╚██████╔╝███████╗██║  ██╗██║   ██║   
╚═╝     ╚═╝  ╚═╝ ╚═════╝  ╚═════╝╚══════╝╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝ ╚═════╝        ╚═╝    ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝   ╚═╝   
                                                                                                                                          
"""

class DismissedEvent(Protocol):
    """Protocol for screen dismissed events"""
    screen: Screen
    value: Any

class FileBrowserDialog(ModalScreen[str]):
    """A modal dialog for browsing and selecting files"""
    
    def __init__(self, start_path: str = str(Path.home())):
        super().__init__()
        self.start_path = start_path
        self.selected_path = None
        
    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Middle():
                yield Label("Select a file:", id="browser_label")
                yield DirectoryTree(self.start_path, id="file_tree")
                with Horizontal():
                    yield Button("Select", id="select_button", variant="primary")
                    yield Button("Cancel", id="cancel_button")
        yield Footer()
    
    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        """Handle file selection"""
        self.selected_path = str(event.path)
        self.dismiss(self.selected_path)
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses"""
        if event.button.id == "select_button":
            if self.selected_path:
                self.dismiss(self.selected_path)
        elif event.button.id == "cancel_button":
            self.dismiss(None)

class FileSelectionScreen(Screen):
    """Screen for selecting input files"""
    
    def __init__(self):
        super().__init__()
        self.selected_file = None
        
    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Middle():
                yield Label("Enter the path to your XLSX or Parquet file:", id="file_label")
                yield Input(placeholder="Enter file path...", id="file_input")
                yield Button("Browse...", id="browse_button")
                yield Button("Continue", id="continue_button", variant="primary")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses"""
        if not event.button or not event.button.id:
            return
            
        if event.button.id == "browse_button":
            # Show file browser dialog
            self.app.push_screen(FileBrowserDialog())
        elif event.button.id == "continue_button":
            file_input = self.query_one("#file_input", Input)
            file_path = file_input.value.strip()
            
            if not file_path:
                self.notify("Please enter a file path", severity="error")
                return
                
            if not os.path.exists(file_path):
                self.notify("File does not exist", severity="error")
                return
                
            file_ext = Path(file_path).suffix.lower()
            if file_ext not in ['.xlsx', '.xls', '.parquet']:
                self.notify("Unsupported file type. Please select an XLSX or Parquet file", severity="error")
                return
                
            self.selected_file = file_path
            app = cast(DataProcessorApp, self.app)
            self.app.push_screen(MainMenu(selected_file=file_path))
    
    def on_screen_dismissed(self, event: Message) -> None:  # type: ignore
        """Handle file browser dialog dismissal"""
        if isinstance(event.screen, FileBrowserDialog) and event.value:  # type: ignore
            file_input = self.query_one("#file_input", Input)
            file_input.value = event.value  # type: ignore

class MainMenu(Screen):
    """Main menu screen for selecting processing methods"""
    
    METHODS = [
        "f1_check_missing_values",
        "f2_ensure_mandatory_fields",
        "f3_standardize_numerical_formats",
        "f4_remove_outdated_data",
        "f5_validate_external_sources",
        "f6_confirm_uniqueness",
        "f7_match_categories",
        "f8_validate_text_fields",
        "f9_ensure_relationships",
        "f10_implement_entry_rules",
        "f11_clean_html_tags",
        "f12_clean_hadoop_tags",
        "f13_clean_boilerplate",
        "f14_legal_domain_filter",
        "f15_legal_domain_classify"
    ]
    
    def __init__(self, selected_file: str):
        super().__init__()
        self.selected_methods = set()
        self.selected_file = selected_file
    
    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Middle():
                yield Label(f"Selected file: {self.selected_file}", id="file_label")
                yield Label("Select processing methods (click to select/deselect):", id="select_label")
                with Vertical(id="methods_container"):
                    for method in self.METHODS:
                        button = Button(method, id=f"btn_{method}")
                        button.add_class("method_button")
                        yield button
                yield Button("Start Processing", id="start_button", variant="primary")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses"""
        if not event.button or not event.button.id:
            return
            
        if event.button.id == "start_button":
            if self.selected_methods:
                app = cast(DataProcessorApp, self.app)
                self.app.push_screen(ProcessingScreen(
                    selected_methods=list(self.selected_methods),
                    spark_session=app.spark_session,
                    config=app.config,
                    input_file=self.selected_file
                ))
        elif event.button.id.startswith("btn_"):
            method = event.button.id[4:]  # Remove "btn_" prefix
            if method in self.selected_methods:
                self.selected_methods.remove(method)
                event.button.remove_class("selected")
            else:
                self.selected_methods.add(method)
                event.button.add_class("selected")

class ProcessingScreen(Screen):
    """Screen for displaying processing progress"""
    
    def __init__(self, selected_methods: List[str], spark_session: SparkSession, config: Dict[str, Any], input_file: str):
        super().__init__()
        self.selected_methods = selected_methods
        self.spark_session = spark_session
        self.config = config
        self.input_file = input_file
        self.processing_complete = False
        self.error_occurred = False
        
    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Middle():
                yield Label(f"Processing file: {self.input_file}", id="file_label")
                yield Label("Processing data...", id="status_label")
                with Vertical(id="progress_container"):
                    for i, method in enumerate(self.selected_methods):
                        yield ProgressBar(id=f"progress_{i}_{method}")
                yield Button("Return to Main Menu", id="return_button", variant="primary", classes="hidden")
        yield Footer()
        
    def on_mount(self) -> None:
        """Start processing when screen is mounted"""
        self.process_data()
        
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses"""
        if event.button.id == "return_button":
            self.app.pop_screen()
        
    def process_data(self) -> None:
        """Process data using selected methods"""
        if not self.spark_session or not self.config:
            raise ValueError("Spark session and config must be provided")
            
        framework = DataQualityFramework(self.spark_session, self.config)
        
        try:
            # Process the file based on its type
            file_ext = Path(self.input_file).suffix.lower()
            if file_ext in ['.xlsx', '.xls']:
                results = framework.process_file(self.input_file)
            else:  # parquet
                results = framework.process_parquet_files([self.input_file])
            
            # Map method names to processor names
            processor_map = {
                'f1_check_missing_values': 'missing_values',
                'f2_ensure_mandatory_fields': 'mandatory_fields',
                'f3_standardize_numerical_formats': 'numerical_formats',
                'f4_remove_outdated_data': 'outdated_data',
                'f5_validate_external_sources': 'external_validation',
                'f6_confirm_uniqueness': 'uniqueness',
                'f7_match_categories': 'categories',
                'f8_validate_text_fields': 'text_validation',
                'f9_ensure_relationships': 'relationships',
                'f10_implement_entry_rules': 'entry_rules',
                'f11_clean_html_tags': 'html_cleaning',
                'f12_clean_hadoop_tags': 'hadoop_cleaning',
                'f13_clean_boilerplate': 'boilerplate_cleaning',
                'f14_legal_domain_filter': 'legal_domain_filter',
                'f15_legal_domain_classify': 'legal_domain_classify'
            }
            
            for i, method in enumerate(self.selected_methods):
                progress_bar = self.query_one(f"#progress_{i}_{method}", ProgressBar)
                progress_bar.update(total=100)
                
                try:
                    # Get the appropriate processor and process the data
                    processor_name = processor_map.get(method)
                    if processor_name and processor_name in framework.processors:
                        processor = framework.processors[processor_name]
                        
                        # Update progress in chunks
                        for progress in range(0, 101, 10):
                            progress_bar.update(progress=progress)
                            time.sleep(0.1)  # Small delay to show progress
                        
                        # Process the data
                        if isinstance(results, dict) and results.get('status') == 'success':
                            processor.process(results)
                        else:
                            raise ValueError("Invalid results format")
                        
                        # Complete the progress bar
                        progress_bar.update(progress=100)
                except Exception as e:
                    self.error_occurred = True
                    status_label = self.query_one("#status_label", Label)
                    status_label.update(f"Error in {method}: {str(e)}")
                    progress_bar.update(progress=0)
                    return
            
            # Show completion message and return button
            if not self.error_occurred:
                status_label = self.query_one("#status_label", Label)
                status_label.update("Processing complete!")
                return_button = self.query_one("#return_button", Button)
                return_button.remove_class("hidden")
                self.processing_complete = True
                
        except Exception as e:
            self.error_occurred = True
            status_label = self.query_one("#status_label", Label)
            status_label.update(f"Error: {str(e)}")
            return_button = self.query_one("#return_button", Button)
            return_button.remove_class("hidden")

class DataProcessorApp(App[None]):
    """Main application class"""
    
    CSS = """
    Screen {
        background: #1f1f1f;
    }
    
    Header {
        background: #2f2f2f;
        color: white;
        text-align: center;
        padding: 1;
    }
    
    Footer {
        background: #2f2f2f;
        color: white;
        text-align: center;
        padding: 1;
    }
    
    Label {
        color: white;
        padding: 1;
    }
    
    Input {
        width: 100%;
        margin: 1;
        background: #3f3f3f;
        color: white;
        border: solid #4f4f4f;
    }
    
    Input:focus {
        border: solid #5f5f5f;
    }
    
    #methods_container {
        height: 70vh;
        overflow-y: auto;
        border: solid #3f3f3f;
        margin: 1;
    }
    
    Button {
        width: 100%;
        margin: 1;
        background: #3f3f3f;
        color: white;
    }
    
    Button:hover {
        background: #4f4f4f;
    }
    
    Button.selected {
        background: #5f5f5f;
    }
    
    Button#start_button {
        background: #2f5f2f;
        margin-top: 2;
    }
    
    Button#start_button:hover {
        background: #3f7f3f;
    }
    
    ProgressBar {
        width: 100%;
        margin: 1;
    }
    """
    
    def __init__(self, spark_session: SparkSession, config: Dict[str, Any]):
        super().__init__()
        self._spark_session = spark_session
        self._config = config
        
    @property
    def spark_session(self) -> SparkSession:
        return self._spark_session
    
    @property
    def config(self) -> Dict[str, Any]:
        return self._config
        
    def on_mount(self) -> None:
        """Display logo and show file selection screen on startup"""
        console = Console()
        console.print(Panel(LOGO, style="bold blue", expand=False))
        self.push_screen(FileSelectionScreen())

def main():
    """Main entry point"""
    # Initialize Spark session with safe configurations
    spark = SparkSession.builder \
        .appName("Teo's Data Processor") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .getOrCreate()
        
    # Default configuration
    config = {
        "checkpoint_dir": "checkpoints",
        "output_dir": "output",
        "batch_size": 1000,
        "missing_value_strategy": "fill",
        "missing_threshold": 0.1,
        # HTML cleaning configuration
        "html_columns": {
            "content": {
                "handle_entities": True
            }
        },
        # Hadoop cleaning configuration
        "hadoop_columns": {
            "logs": {
                "remove_metadata": True
            }
        },
        # Boilerplate cleaning configuration
        "boilerplate_columns": {
            "text": {
                "remove_duplicates": True,
                "remove_header_footer": True,
                "custom_patterns": [
                    r'^\s*Internal Use Only.*?$',
                    r'^\s*Confidential Document.*?$'
                ]
            }
        }
    }
    
    # Run the application
    app = DataProcessorApp(spark, config)
    app.run()

if __name__ == "__main__":
    main() 
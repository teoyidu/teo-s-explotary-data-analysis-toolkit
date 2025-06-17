from textual.app import App, ComposeResult
from textual.containers import Center, Middle, Vertical
from textual.widgets import Button, ProgressBar, Select, Header, Footer, Label, ListView, ListItem, DataTable
from textual.screen import Screen
from rich.console import Console
from rich.panel import Panel
import time
from typing import List, Dict, Any
from data_quality_framework import DataQualityFramework
from pyspark.sql import SparkSession

# ASCII Art Logo
LOGO = """
████████╗███████╗ ██████╗ ███████╗    ██████╗  █████╗ ████████╗ █████╗                                                                    
╚══██╔══╝██╔════╝██╔═══██╗██╔════╝    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗                                                                   
   ██║   █████╗  ██║   ██║███████╗    ██║  ██║███████║   ██║   ███████║                                                                   
   ██║   ██╔══╝  ██║   ██║╚════██║    ██║  ██║██╔══██║   ██║   ██╔══██║                                                                   
   ██║   ███████╗╚██████╔╝███████║    ██████╔╝██║  ██║   ██║   ██║  ██║                                                                   
   ╚═╝   ╚══════╝ ╚═════╝ ╚══════╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝                                                                   
                                                                                                                                          
██████╗ ██████╗  ██████╗  ██████╗███████╗███████╗███████╗██╗███╗   ██╗ ██████╗     ████████╗ ██████╗  ██████╗ ██╗     ██╗  ██╗██╗████████╗
██╔══██╗██╔══██╗██╔═══██╗██╔════╝██╔════╝██╔════╝██╔════╝██║████╗  ██║██╔════╝     ╚══██╔══╝██╔═══██╗██╔═══██╗██║     ██║ ██╔╝██║╚══██╔══╝
██████╔╝██████╔╝██║   ██║██║     █████╗  ███████╗███████╗██║██╔██╗ ██║██║  ███╗       ██║   ██║   ██║██║   ██║██║     █████╔╝ ██║   ██║   
██╔═══╝ ██╔══██╗██║   ██║██║     ██╔══╝  ╚════██║╚════██║██║██║╚██╗██║██║   ██║       ██║   ██║   ██║██║   ██║██║     ██╔═██╗ ██║   ██║   
██║     ██║  ██║╚██████╔╝╚██████╗███████╗███████║███████║██║██║ ╚████║╚██████╔╝       ██║   ╚██████╔╝╚██████╔╝███████╗██║  ██╗██║   ██║   
╚═╝     ╚═╝  ╚═╝ ╚═════╝  ╚═════╝╚══════╝╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝ ╚═════╝        ╚═╝    ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝   ╚═╝   
                                                                                                                                          
"""

class ProcessingScreen(Screen):
    """Screen for processing data with progress bars"""
    
    def __init__(self, selected_methods: List[str], spark_session: SparkSession, config: Dict[str, Any]):
        super().__init__()
        self.selected_methods = selected_methods
        self.spark_session = spark_session
        self.config = config
        self.current_method = 0
        self.progress_bars = {}
        
    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Middle():
                for i, method in enumerate(self.selected_methods):
                    yield ProgressBar(id=f"progress_{i}_{method}")
        yield Footer()
        
    def on_mount(self) -> None:
        """Start processing when screen is mounted"""
        self.process_data()
        
    def process_data(self) -> None:
        """Process data using selected methods"""
        framework = DataQualityFramework(self.spark_session, self.config)
        
        for i, method in enumerate(self.selected_methods):
            progress_bar = self.query_one(f"#progress_{i}_{method}", ProgressBar)
            progress_bar.update(total=100)
            
            # Simulate processing with progress updates
            for j in range(101):
                time.sleep(0.05)  # Simulate work
                progress_bar.update(progress=j)
                
            # Call actual processing method
            method_func = getattr(framework, method)
            method_func()  # You'll need to pass appropriate parameters here

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
        "f13_clean_boilerplate"
    ]
    
    def __init__(self):
        super().__init__()
        self.selected_methods = set()
    
    def compose(self) -> ComposeResult:
        yield Header()
        with Center():
            with Middle():
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
        if event.button.id == "start_button":
            if self.selected_methods:
                self.app.push_screen(ProcessingScreen(
                    selected_methods=list(self.selected_methods),
                    spark_session=self.app.spark_session,
                    config=self.app.config
                ))
        elif event.button.id.startswith("btn_"):
            method = event.button.id[4:]  # Remove "btn_" prefix
            if method in self.selected_methods:
                self.selected_methods.remove(method)
                event.button.remove_class("selected")
            else:
                self.selected_methods.add(method)
                event.button.add_class("selected")

class DataProcessorApp(App):
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
        self.spark_session = spark_session
        self.config = config
        
    def on_mount(self) -> None:
        """Display logo and show main menu on startup"""
        console = Console()
        console.print(Panel(LOGO, style="bold blue", expand=False))
        self.push_screen(MainMenu())

def main():
    """Main entry point"""
    # Initialize Spark session with minimal configuration
    spark = SparkSession.builder \
        .appName("Teo's Data Processor") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
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
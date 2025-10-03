import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import re
from pathlib import Path
import config_py  # Import the shared configuration

# Set the style for the plots
plt.style.use('ggplot')
sns.set_context("paper")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['pdf.fonttype'] = 42  # Ensure text is editable in the PDF

# Define input and output directories
input_dir = os.path.join(config_py.RESULTS_DIR, "tagged_csv")
output_dir = os.path.join(config_py.RESULTS_DIR, "figure")

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

def sanitize_filename(name):
    """Convert column name to a safe filename by replacing special characters."""
    # Replace spaces, parentheses, and slashes with underscores
    safe_name = re.sub(r'[\s\(\)/\\]', '_', name)
    # Remove any remaining special characters
    safe_name = re.sub(r'[^\w\-\.]', '', safe_name)
    return safe_name

def inspect_csv(csv_path):
    """Inspect a CSV file and print its columns and first few rows for debugging."""
    df = pd.read_csv(csv_path)
    print(f"Columns: {df.columns.tolist()}")
    print(f"First 2 rows: \n{df.head(2)}")
    return df

def get_categorical_columns(df):
    """Find potential categorical columns for grouping."""
    # Look for string columns or columns with few unique values
    categorical = []
    for col in df.columns:
        if df[col].dtype == 'object' or (pd.api.types.is_numeric_dtype(df[col]) and df[col].nunique() < 10):
            categorical.append(col)
    return categorical

def find_object_column(df, categorical_cols):
    """Find the column that likely represents different object types/data structures."""
    # Common names for object/data structure columns
    object_names = ['object_id', 'object', 'ds', 'datastructure', 'algorithm', 'impl', 'implementation']
    
    # First check for exact matches
    for col in categorical_cols:
        if col.lower() in object_names:
            return col
    
    # Then check for partial matches
    for col in categorical_cols:
        for name in object_names:
            if name in col.lower():
                return col
    
    # If no match found, return the first categorical column that's not 'case_id'
    for col in categorical_cols:
        if col != 'case_id':
            return col
    
    # Fallback to the first categorical column
    if categorical_cols:
        return categorical_cols[0]
    
    return None

def plot_load_factor_support(csv_path, output_dir):
    """Plot load factor support results with objects compared on the same figure."""
    df = pd.read_csv(csv_path)
    
    print(f"Inspecting load factor support CSV:")
    categorical_cols = get_categorical_columns(df)
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in categorical_cols]
    
    print(f"Categorical columns: {categorical_cols}")
    print(f"Numeric columns: {numeric_cols}")
    
    # Find the object column (data structure identifier)
    object_col = find_object_column(df, categorical_cols)
    
    if not object_col:
        print("No suitable object/data structure column found")
        return
    
    # Find load factor columns
    load_factor_cols = [col for col in numeric_cols if 'load' in col.lower() or 'factor' in col.lower()]
    
    # If no explicit load factor column, use all numeric columns
    if not load_factor_cols:
        load_factor_cols = numeric_cols
    
    for col in load_factor_cols:
        plt.figure(figsize=(14, 8))
        
        # Plot all objects in one figure for direct comparison
        sns.barplot(data=df, x=object_col, y=col)
        
        plt.title(f'{col} Comparison Across Different Implementations')
        plt.xlabel('Implementation')
        plt.ylabel(col)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        safe_filename = sanitize_filename(f'comparison_load_factor_{col}')
        plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
        plt.close()

def plot_micro_benchmarks(csv_path, output_dir):
    """Plot micro benchmark results with objects compared on the same figure."""
    df = pd.read_csv(csv_path)
    
    print(f"Inspecting micro benchmarks CSV:")
    categorical_cols = get_categorical_columns(df)
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in categorical_cols]
    
    print(f"Categorical columns: {categorical_cols}")
    print(f"Numeric columns: {numeric_cols}")
    
    # Find the object column and case/operation column
    object_col = find_object_column(df, categorical_cols)
    
    if not object_col:
        print("No suitable object/data structure column found")
        return
    
    # Try to find operation column (if it exists)
    op_col = None
    for col in categorical_cols:
        if col != object_col and ('op' in col.lower() or 'case' in col.lower()):
            op_col = col
            break
    
    # Plot each numeric metric
    for metric in numeric_cols:
        plt.figure(figsize=(14, 8))
        
        if op_col:
            # Group by both object and operation type
            sns.barplot(data=df, x=object_col, y=metric, hue=op_col)
            plt.title(f'{metric} Comparison by Implementation and {op_col}')
            plt.legend(title=op_col)
        else:
            # Just group by object
            sns.barplot(data=df, x=object_col, y=metric)
            plt.title(f'{metric} Comparison Across Different Implementations')
        
        plt.xlabel('Implementation')
        plt.ylabel(metric)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        safe_filename = sanitize_filename(f'comparison_micro_{metric}')
        plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
        plt.close()

def plot_progressive_latency(csv_path, output_dir):
    """Plot progressive latency results with separate graphs for query hits, query misses, and insertions."""
    df = pd.read_csv(csv_path)
    
    print(f"Inspecting progressive latency CSV:")
    categorical_cols = get_categorical_columns(df)
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in categorical_cols]
    
    print(f"Categorical columns: {categorical_cols}")
    print(f"Numeric columns: {numeric_cols}")
    
    # Find the object column (data structure identifier)
    object_col = find_object_column(df, categorical_cols)
    
    if not object_col:
        print("No suitable object/data structure column found")
        return
    
    # Create three separate graphs: query hits, query misses, and insertions
    
    # 1. Plot for Query Hits
    query_hit_df = df[df['case_id'] == 'QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR']
    if not query_hit_df.empty:
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=query_hit_df, x='load_factor', y='query_latency (ns/op)', hue=object_col, marker='o')
        plt.title('Query Latency for Hit Operations vs Load Factor')
        plt.xlabel('Load Factor')
        plt.ylabel('Query Latency (ns/op)')
        plt.legend(title='Implementation')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'query_hit_latency.pdf'), format='pdf')
        plt.close()
        
        # Optional: Also plot throughput
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=query_hit_df, x='load_factor', y='query_throughput (ops/s)', hue=object_col, marker='o')
        plt.title('Query Throughput for Hit Operations vs Load Factor')
        plt.xlabel('Load Factor')
        plt.ylabel('Query Throughput (ops/s)')
        plt.legend(title='Implementation')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'query_hit_throughput.pdf'), format='pdf')
        plt.close()
    
    # 2. Plot for Query Misses
    query_miss_df = df[df['case_id'] == 'QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR']
    if not query_miss_df.empty:
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=query_miss_df, x='load_factor', y='query_latency (ns/op)', hue=object_col, marker='o')
        plt.title('Query Latency for Miss Operations vs Load Factor')
        plt.xlabel('Load Factor')
        plt.ylabel('Query Latency (ns/op)')
        plt.legend(title='Implementation')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'query_miss_latency.pdf'), format='pdf')
        plt.close()
        
        # Optional: Also plot throughput
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=query_miss_df, x='load_factor', y='query_throughput (ops/s)', hue=object_col, marker='o')
        plt.title('Query Throughput for Miss Operations vs Load Factor')
        plt.xlabel('Load Factor')
        plt.ylabel('Query Throughput (ops/s)')
        plt.legend(title='Implementation')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'query_miss_throughput.pdf'), format='pdf')
        plt.close()
    
    # 3. Plot for Insertions (combining both datasets since insert performance is the same)
    # Using query_hit_df as the base, but we could use either one since both contain insertion data
    if not df.empty:
        # Create a set of unique combinations of object_id and load_factor
        combined_df = df.drop_duplicates(subset=[object_col, 'load_factor'])
        
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=combined_df, x='load_factor', y='insert_latency (ns/op)', hue=object_col, marker='o')
        plt.title('Insert Latency vs Load Factor')
        plt.xlabel('Load Factor')
        plt.ylabel('Insert Latency (ns/op)')
        plt.legend(title='Implementation')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'insert_latency.pdf'), format='pdf')
        plt.close()
        
        # Optional: Also plot throughput
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=combined_df, x='load_factor', y='insert_throughput (ops/s)', hue=object_col, marker='o')
        plt.title('Insert Throughput vs Load Factor')
        plt.xlabel('Load Factor')
        plt.ylabel('Insert Throughput (ops/s)')
        plt.legend(title='Implementation')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'insert_throughput.pdf'), format='pdf')
        plt.close()

def plot_scaling_results(csv_path, output_dir):
    """Plot scaling results with all objects on the same figure."""
    df = pd.read_csv(csv_path)
    
    print(f"Inspecting scaling results CSV:")
    categorical_cols = get_categorical_columns(df)
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in categorical_cols]
    
    print(f"Categorical columns: {categorical_cols}")
    print(f"Numeric columns: {numeric_cols}")
    
    # Find the object column (data structure identifier)
    object_col = find_object_column(df, categorical_cols)
    
    if not object_col:
        print("No suitable object/data structure column found")
        return
    
    # Look for scaling factor column (threads, size, etc.)
    scaling_cols = [col for col in numeric_cols if any(term in col.lower() for term in 
                                                      ['thread', 'size', 'scale', 'core', 'worker'])]
    
    # If no explicit scaling column, use any numeric column with small number of unique values
    if not scaling_cols:
        for col in numeric_cols:
            if df[col].nunique() < 20:  # Assuming scaling factors typically have fewer unique values
                scaling_cols = [col]
                break
    
    # If still no scaling column, use the first numeric column
    if not scaling_cols and numeric_cols:
        scaling_cols = [numeric_cols[0]]
    
    # Use the remaining columns as metrics to plot
    metric_cols = [col for col in numeric_cols if col not in scaling_cols]
    
    # For each scaling factor and metric, create line plots with all objects
    for x_col in scaling_cols:
        for y_col in metric_cols:
            plt.figure(figsize=(14, 8))
            
            # Plot all objects as different lines on the same figure
            sns.lineplot(data=df, x=x_col, y=y_col, hue=object_col, marker='o')
            
            plt.title(f'{y_col} vs {x_col} Scaling Comparison')
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.tight_layout()
            
            # Add a legend with the object names
            plt.legend(title='Implementation')
            
            safe_filename = sanitize_filename(f'comparison_scaling_{y_col}_vs_{x_col}')
            plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
            plt.close()

def plot_throughput_space_efficiency(csv_path, output_dir):
    """Plot throughput and space efficiency results with all objects compared."""
    df = pd.read_csv(csv_path)
    
    print(f"Inspecting throughput/space efficiency CSV:")
    categorical_cols = get_categorical_columns(df)
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in categorical_cols]
    
    print(f"Categorical columns: {categorical_cols}")
    print(f"Numeric columns: {numeric_cols}")
    
    # Find the object column (data structure identifier)
    object_col = find_object_column(df, categorical_cols)
    
    if not object_col:
        print("No suitable object/data structure column found")
        return
    
    # Look for throughput and space efficiency metrics
    throughput_cols = [col for col in numeric_cols if any(term in col.lower() for term in 
                                                         ['throughput', 'ops', 'tps', 'qps'])]
    space_cols = [col for col in numeric_cols if any(term in col.lower() for term in 
                                                    ['space', 'memory', 'size', 'bytes', 'mb'])]
    
    # If no explicit throughput columns, look for performance metrics
    if not throughput_cols:
        throughput_cols = [col for col in numeric_cols if col not in space_cols and 
                           any(term in col.lower() for term in ['performance', 'speed', 'time', 'latency'])]
    
    # If still no throughput columns, use remaining numeric columns
    if not throughput_cols:
        throughput_cols = [col for col in numeric_cols if col not in space_cols]
    
    # Plot bar charts for each metric grouped by object
    for col in throughput_cols + space_cols:
        plt.figure(figsize=(14, 8))
        
        # Plot all objects in one figure for direct comparison
        sns.barplot(data=df, x=object_col, y=col)
        
        plt.title(f'{col} Comparison Across Different Implementations')
        plt.xlabel('Implementation')
        plt.ylabel(col)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        safe_filename = sanitize_filename(f'comparison_{col}')
        plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
        plt.close()
    
    # Create a combined throughput vs space scatter plot if both metrics exist
    if throughput_cols and space_cols:
        # Check if there's a load factor or other grouping variable
        load_factor_cols = [col for col in numeric_cols if 'load' in col.lower() or 'factor' in col.lower()]
        
        if load_factor_cols:
            # Multiple points per object (at different load factors)
            plt.figure(figsize=(14, 8))
            sns.lineplot(data=df, x=load_factor_cols[0], y=throughput_cols[0], hue=object_col, marker='o')
            plt.title(f'{throughput_cols[0]} vs {load_factor_cols[0]} Comparison')
            plt.xlabel(load_factor_cols[0])
            plt.ylabel(throughput_cols[0])
            plt.tight_layout()
            plt.legend(title='Implementation')
            safe_filename = sanitize_filename(f'comparison_throughput_vs_load_factor')
            plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
            plt.close()
            
            # Also create a space vs load factor plot
            plt.figure(figsize=(14, 8))
            sns.lineplot(data=df, x=load_factor_cols[0], y=space_cols[0], hue=object_col, marker='o')
            plt.title(f'{space_cols[0]} vs {load_factor_cols[0]} Comparison')
            plt.xlabel(load_factor_cols[0])
            plt.ylabel(space_cols[0])
            plt.tight_layout()
            plt.legend(title='Implementation')
            safe_filename = sanitize_filename(f'comparison_space_vs_load_factor')
            plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
            plt.close()

def plot_ycsb_results(csv_path, output_dir):
    """
    Plot YCSB benchmark results with two separate figures:
    - entry_id=0 for data without resize
    - entry_id=1 for data with resize
    """
    df = pd.read_csv(csv_path)
    
    print(f"Inspecting YCSB results CSV:")
    categorical_cols = get_categorical_columns(df)
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in categorical_cols]
    
    print(f"Categorical columns: {categorical_cols}")
    print(f"Numeric columns: {numeric_cols}")
    
    # Find the object column (data structure identifier)
    object_col = find_object_column(df, categorical_cols)
    
    if not object_col:
        print("No suitable object/data structure column found")
        return
    
    # Try to identify workload column
    workload_cols = [col for col in categorical_cols if 'workload' in col.lower() or 'case' in col.lower()]
    
    if not workload_cols and len(categorical_cols) > 1:
        # If no explicit workload column, use another categorical column that's not the object column
        workload_cols = [col for col in categorical_cols if col != object_col and col != 'entry_id']
    
    # Check if entry_id exists in the data
    has_entry_id = 'entry_id' in df.columns
    
    if has_entry_id:
        # Get the unique entry_id values
        entry_ids = df['entry_id'].unique()
        
        # Create separate plots for each entry_id
        for entry_id in entry_ids:
            # Filter data for this entry_id
            entry_df = df[df['entry_id'] == entry_id]
            
            resize_label = "without resize" if entry_id == 0 else "with resize"
            
            if workload_cols:
                workload_col = workload_cols[0]
                
                for metric in numeric_cols:
                    plt.figure(figsize=(14, 10))
                    
                    # Create a grouped bar chart
                    sns.barplot(data=entry_df, x=workload_col, y=metric, hue=object_col)
                    
                    plt.title(f'{metric} Comparison by Workload and Implementation ({resize_label})')
                    plt.xlabel('Workload')
                    plt.ylabel(metric)
                    plt.legend(title='Implementation')
                    plt.tight_layout()
                    
                    safe_filename = sanitize_filename(f'comparison_ycsb_{metric}_by_workload_entry{entry_id}')
                    plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
                    plt.close()
            else:
                # No workload column, just compare objects
                for metric in numeric_cols:
                    plt.figure(figsize=(14, 8))
                    
                    # Plot all objects in one figure for direct comparison
                    sns.barplot(data=entry_df, x=object_col, y=metric)
                    
                    plt.title(f'{metric} Comparison Across Different Implementations ({resize_label})')
                    plt.xlabel('Implementation')
                    plt.ylabel(metric)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    
                    safe_filename = sanitize_filename(f'comparison_ycsb_{metric}_entry{entry_id}')
                    plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
                    plt.close()
    else:
        # No entry_id column, process as before
        if workload_cols:
            workload_col = workload_cols[0]
            
            for metric in numeric_cols:
                plt.figure(figsize=(14, 10))
                
                # Create a grouped bar chart
                sns.barplot(data=df, x=workload_col, y=metric, hue=object_col)
                
                plt.title(f'{metric} Comparison by Workload and Implementation')
                plt.xlabel('Workload')
                plt.ylabel(metric)
                plt.legend(title='Implementation')
                plt.tight_layout()
                
                safe_filename = sanitize_filename(f'comparison_ycsb_{metric}_by_workload')
                plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
                plt.close()
        else:
            # No workload column, just compare objects
            for metric in numeric_cols:
                plt.figure(figsize=(14, 8))
                
                # Plot all objects in one figure for direct comparison
                sns.barplot(data=df, x=object_col, y=metric)
                
                plt.title(f'{metric} Comparison Across Different Implementations')
                plt.xlabel('Implementation')
                plt.ylabel(metric)
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                safe_filename = sanitize_filename(f'comparison_ycsb_{metric}')
                plt.savefig(os.path.join(output_dir, f'{safe_filename}.pdf'), format='pdf')
                plt.close()

def main():
    """Process all CSV files and create plots."""
    # Map CSV files to plotting functions
    csv_plot_mapping = {
        'load_factor_support_results.csv': plot_load_factor_support,
        'micro_results.csv': plot_micro_benchmarks,
        'progressive_latency_results.csv': plot_progressive_latency,
        'scaling_results.csv': plot_scaling_results,
        'throughput_space_eff_results.csv': plot_throughput_space_efficiency,
        'ycsb_results.csv': plot_ycsb_results
    }
    
    # Process each CSV file
    for filename, plot_func in csv_plot_mapping.items():
        csv_path = os.path.join(input_dir, filename)
        if os.path.exists(csv_path):
            print(f"\nProcessing {filename}...")
            try:
                # First inspect the CSV
                df = inspect_csv(csv_path)
                # Then plot
                plot_func(csv_path, output_dir)
                print(f"Successfully created plots for {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"File {filename} not found")
    
    print(f"\nAll plots saved to {output_dir}")

if __name__ == "__main__":
    main()

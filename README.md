import pandas as pd
import os

def combine_excel_sheets(input_folder, output_file):
    all_dataframes = []

    # Get a list of all Excel files in the input folder
    excel_files = [file for file in os.listdir(input_folder) if file.endswith('.xlsx')]

    for excel_file in excel_files:
        file_path = os.path.join(input_folder, excel_file)

        # Read all sheets from the current Excel file into a dictionary of dataframes
        excel_data = pd.read_excel(file_path, sheet_name=None)

        # Append all dataframes from the current Excel file to the list
        for sheet_name, df in excel_data.items():
            all_dataframes.append((sheet_name, df))

    # Create a Pandas Excel writer using XlsxWriter as the engine
    writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

    # Write each dataframe to a separate Excel sheet in the output file
    for sheet_name, df in all_dataframes:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save the Excel file
    writer.save()

    print(f'Combined {len(all_dataframes)} sheets from {len(excel_files)} files into "{output_file}".')

if __name__ == "__main__":
    input_folder = "path/to/input_folder"  # Replace with the path to the folder containing input Excel files
    output_file = "path/to/output_file.xlsx"  # Replace with the desired path for the output Excel file

    combine_excel_sheets(input_folder, output_file)

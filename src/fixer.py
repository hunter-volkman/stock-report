import zipfile
import os
import shutil
import xml.etree.ElementTree as ET
import re

import xml.etree.ElementTree as ET

import xml.etree.ElementTree as ET

def update_excel_xml(excel_path, num_data_rows, sheet_mappings):
    """
    Updates the Excel workbook XML to dynamically adjust formulas and clear excess data.

    Args:
        excel_path (str): Path to the Excel file.
        num_data_rows (int): Number of data rows in 'Raw Import'.
        sheet_mappings (dict): Mapping of sheet names to XML file names.
    """
    temp_dir = "temp_excel"

    # Extract the Excel ZIP archive
    with zipfile.ZipFile(excel_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)

    # Modify the necessary sheet XML files
    for sheet_name in ["Sorted Raw", "Calibrated Values", "Bounded Calibrated"]:
        if sheet_name not in sheet_mappings:
            print(f"⚠ Warning: Sheet '{sheet_name}' not found in workbook. Skipping...")
            continue
        
        sheet_xml_path = os.path.join(temp_dir, "xl", "worksheets", sheet_mappings[sheet_name])
        tree = ET.parse(sheet_xml_path)
        root = tree.getroot()
        namespace = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

        # Locate sheet data section
        sheet_data = root.find(".//ns:sheetData", namespace)
        if sheet_data is None:
            continue

        # Remove extra rows beyond num_data_rows + 1
        excess_rows = []
        for row in sheet_data.findall("ns:row", namespace):
            row_number = int(row.attrib.get("r", "0"))
            if row_number > num_data_rows + 1:
                excess_rows.append(row)

        for row in excess_rows:
            sheet_data.remove(row)
            print(f"🗑 Removed stale row {row.attrib.get('r')} from {sheet_name}")

        # Update Sorted Raw formula (only for A2)
        if sheet_name == "Sorted Raw":
            found = False
            for cell in root.findall(".//ns:c", namespace):
                if cell.attrib.get("r") == "A2":
                    found = True
                    formula_element = cell.find(".//ns:f", namespace)
                    if formula_element is None:
                        formula_element = ET.SubElement(cell, "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}f")
                    
                    # Correct Excel-recognized formula insertion
                    formula_element.text = f"_xlfn._xlws.SORT('Raw Import'!A2:X{num_data_rows + 1},1,1)"
                    formula_element.set("t", "array")  # Mark as array formula
                    formula_element.set("ref", f"A2:X{num_data_rows + 1}")  # Correct reference

                    # Ensure A2 has a placeholder value to prevent stripping
                    value_element = cell.find(".//ns:v", namespace)
                    if value_element is None:
                        value_element = ET.SubElement(cell, "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
                    value_element.text = "0"  # Dummy value

            if not found:
                # Create a new A2 cell with the correct formula
                new_cell = ET.Element("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c", r="A2")
                formula_element = ET.SubElement(new_cell, "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}f")
                formula_element.text = f"_xlfn._xlws.SORT('Raw Import'!A2:X{num_data_rows + 1},1,1)"
                formula_element.set("t", "array")
                formula_element.set("ref", f"A2:X{num_data_rows + 1}")

                # Add a dummy value to prevent Excel from deleting it
                value_element = ET.SubElement(new_cell, "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
                value_element.text = "0"

                sheet_data.insert(1, new_cell)

            print(f"✅ Updated Sorted Raw formula: _xlfn._xlws.SORT('Raw Import'!A2:X{num_data_rows + 1},1,1)")

        # Save the modified XML
        tree.write(sheet_xml_path, encoding="UTF-8", xml_declaration=True)

    # Repackage the modified files into a new .xlsx
    updated_excel_path = excel_path.replace(".xlsx", "_final.xlsx")
    with zipfile.ZipFile(updated_excel_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
        for root_dir, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root_dir, file)
                zip_ref.write(file_path, os.path.relpath(file_path, temp_dir))

    # Cleanup
    shutil.rmtree(temp_dir)
    
    print(f"✅ Updated Excel file saved at: {updated_excel_path}")
    return updated_excel_path


def get_sheet_mappings(excel_path):
    """
    Extracts the correct mapping of sheet names to their actual XML filenames.
    
    Args:
        excel_path (str): Path to the Excel file.

    Returns:
        dict: Mapping of sheet names to actual worksheet XML file names.
    """
    temp_dir = "temp_excel"
    with zipfile.ZipFile(excel_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)

    workbook_xml_path = os.path.join(temp_dir, "xl", "workbook.xml")
    rels_xml_path = os.path.join(temp_dir, "xl", "_rels", "workbook.xml.rels")

    sheet_mapping = {}

    # Parse workbook.xml to get sheet names and their relationship IDs
    tree = ET.parse(workbook_xml_path)
    root = tree.getroot()
    namespace = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    sheet_rel_map = {}  # Stores r:id to sheet name
    for sheet in root.findall(".//ns:sheets/ns:sheet", namespace):
        sheet_name = sheet.attrib["name"]
        sheet_rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        sheet_rel_map[sheet_rel_id] = sheet_name  # Map r:id to sheet name

    # Parse workbook.xml.rels to get the actual sheetX.xml filenames
    tree = ET.parse(rels_xml_path)
    root = tree.getroot()
    namespace = {"ns": "http://schemas.openxmlformats.org/package/2006/relationships"}

    for rel in root.findall(".//ns:Relationship", namespace):
        rel_id = rel.attrib["Id"]
        target = rel.attrib["Target"]  # Example: "worksheets/sheet1.xml"

        # Make sure we only process worksheet files, not chartsheets
        if rel_id in sheet_rel_map and "worksheets" in target:
            sheet_name = sheet_rel_map[rel_id]
            sheet_mapping[sheet_name] = os.path.basename(target)  # Extract only the file name

    shutil.rmtree(temp_dir)  # Clean up
    return sheet_mapping

# Example usage
excel_file = "3895th_031125.xlsx"
sheet_mappings = get_sheet_mappings(excel_file)
print(sheet_mappings)
updated_excel = update_excel_xml(excel_file, num_data_rows=141, sheet_mappings=sheet_mappings)

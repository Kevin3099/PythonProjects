import xml.etree.ElementTree as ET
import json
import pandas as pd
import os
import openpyxl

def streaming_parse_medicine_adjusted(file_path, json_output, excel_output):
    print("Starting to parse XML data...")
    context = ET.iterparse(file_path, events=("end",))
    medicines_list = []
    BATCH_SIZE = 1000

    for _, elem in context:
        if elem.tag == 'medicalInformation':
            medicine_data = {}
            medicine_data["title"] = elem.find('title').text
            medicine_data["authHolder"] = elem.find('authHolder').text
            medicine_data["atcCode"] = elem.find('atcCode').text if elem.find('atcCode') is not None else None
            medicine_data["substances"] = elem.find('substances').text if elem.find('substances') is not None else None
            medicine_data["authNrs"] = elem.find('authNrs').text if elem.find('authNrs') is not None else None
            medicine_data["remark"] = elem.find('remark').text if elem.find('remark') is not None else None
         #   medicine_data["style"] = elem.find('style').text if elem.find('style') is not None else None
         #   medicine_data["content"] = elem.find('content').text if elem.find('content') is not None else None

            # Extracting sections
            sections_dict = {}
            for section in elem.findall('.//sections/section'):
                sections_dict[section.find('title').text] = section.text
            medicine_data["sections"] = sections_dict

            # Extracting attributes
         #   medicine_data["type"] = elem.attrib.get("type")
         #   medicine_data["version"] = elem.attrib.get("version")
         #   medicine_data["lang"] = elem.attrib.get("lang")
         #   medicine_data["safetyRelevant"] = elem.attrib.get("safetyRelevant") == "true"
         #   medicine_data["informationUpdate"] = elem.attrib.get("informationUpdate")

            medicines_list.append(medicine_data)

            if len(medicines_list) == BATCH_SIZE:
                print(f"Processing batch of {BATCH_SIZE} records...")
                append_to_files(medicines_list, json_output, excel_output)
                medicines_list = []
                break #only does first batch for testing

            elem.clear()

    if medicines_list:
        print(f"Processing final batch of {len(medicines_list)} records...")
        append_to_files(medicines_list, json_output, excel_output)

def append_to_files(medicines_list, json_output, excel_output):
    with open(json_output, 'a') as file:
        json.dump(medicines_list, file, indent=4)

    # Prepare the data to be appended
    rows = []
    for medicine in medicines_list:
        for section_title, section_content in medicine["sections"].items():
            rows.append([
                medicine["title"], medicine["authHolder"], medicine["atcCode"], medicine["substances"],
                medicine["authNrs"], medicine["remark"], #medicine["style"], medicine["content"], 
                section_title, section_content, #medicine["type"], medicine["version"], medicine["lang"], 
               # medicine["safetyRelevant"], medicine["informationUpdate"]
            ])

    df_new = pd.DataFrame(rows, columns=[
        "Title", "AuthHolder", "ATC Code", "Substances", "AuthNrs", "Remark", "Style", "Content", 
        "Section Title", "Section Content", "Type", "Version", "Language", "Safety Relevant", "Information Update"
    ])

    if os.path.exists(excel_output):
        df_existing = pd.read_excel(excel_output)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_excel(excel_output, index=False)
    print(f"Data successfully appended to {json_output} and {excel_output}!")


streaming_parse_medicine_adjusted('E:/tempImages/AipsDownload_20231006.xml', 'E:/tempImages/output.json', 'E:/tempImages/output.xlsx')
print("Processing complete!")


# import xml.etree.ElementTree as ET
# import json
# import pandas as pd
# import os
# import openpyxl

# def streaming_parse_medicine_adjusted(file_path, json_output, excel_output):
#     print("Starting to parse XML data...")
#     context = ET.iterparse(file_path, events=("end",))
#     medicines_list = []
#     BATCH_SIZE = 1000

#     for _, elem in context:
#         if elem.tag == 'medicalInformation':
#             medicine_data = {}
#             medicine_data["title"] = elem.find('title').text
#             medicine_data["authHolder"] = elem.find('authHolder').text
#             medicine_data["atcCode"] = elem.find('atcCode').text if elem.find('atcCode') is not None else None
#             medicine_data["substances"] = elem.find('substances').text if elem.find('substances') is not None else None
#             medicine_data["authNrs"] = elem.find('authNrs').text if elem.find('authNrs') is not None else None
#             medicine_data["remark"] = elem.find('remark').text if elem.find('remark') is not None else None
#             medicine_data["style"] = elem.find('style').text if elem.find('style') is not None else None
#             medicine_data["content"] = elem.find('content').text if elem.find('content') is not None else None

#             # Extracting sections
#             sections = []
#             for section in elem.findall('.//sections/section'):
#                 section_data = {"title": section.find('title').text, "id": section.attrib.get('id')}
#                 sections.append(section_data)
#             medicine_data["sections"] = sections

#             # Extracting attributes
#             medicine_data["type"] = elem.attrib.get("type")
#             medicine_data["version"] = elem.attrib.get("version")
#             medicine_data["lang"] = elem.attrib.get("lang")
#             medicine_data["safetyRelevant"] = elem.attrib.get("safetyRelevant") == "true"
#             medicine_data["informationUpdate"] = elem.attrib.get("informationUpdate")

#             medicines_list.append(medicine_data)

#             if len(medicines_list) == BATCH_SIZE:
#                 print(f"Processing batch of {BATCH_SIZE} records...")
#                 append_to_files(medicines_list, json_output, excel_output)
#                 medicines_list = []

#             elem.clear()

#     if medicines_list:
#         print(f"Processing final batch of {len(medicines_list)} records...")
#         append_to_files(medicines_list, json_output, excel_output)

# def append_to_files(medicines_list, json_output, excel_output):
#     with open(json_output, 'a') as file:
#         json.dump(medicines_list, file, indent=4)

#     # Prepare the data to be appended
#     rows = []
#     for medicine in medicines_list:
#         for section in medicine["sections"]:
#             rows.append([
#                 medicine["title"], medicine["authHolder"], medicine["atcCode"], medicine["substances"],
#                 medicine["authNrs"], medicine["remark"], medicine["style"], medicine["content"], 
#                 section["title"], section["id"], medicine["type"], medicine["version"], medicine["lang"], 
#                 medicine["safetyRelevant"], medicine["informationUpdate"]
#             ])

#     df_new = pd.DataFrame(rows, columns=[
#         "Title", "AuthHolder", "ATC Code", "Substances", "AuthNrs", "Remark", "Style", "Content", 
#         "Section Title", "Section ID", "Type", "Version", "Language", "Safety Relevant", "Information Update"
#     ])

#     if os.path.exists(excel_output):
#         df_existing = pd.read_excel(excel_output)
#         df_combined = pd.concat([df_existing, df_new], ignore_index=True)
#     else:
#         df_combined = df_new

#     df_combined.to_excel(excel_output, index=False)
#     print(f"Data successfully appended to {json_output} and {excel_output}!")


# streaming_parse_medicine_adjusted('C:/tempImages/AipsDownload_20231006.xml', 'output.json', 'output.xlsx')
# print("Processing complete!")

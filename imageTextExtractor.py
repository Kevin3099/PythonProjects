import pytesseract
from PIL import Image
import os

def extract_text_from_image(image_path):
    """Extract text from the given image path using Tesseract OCR."""
    with Image.open(image_path) as img:
        return pytesseract.image_to_string(img, config='--psm 6')

def extract_text_from_folder(folder_path):
    """Extract text from all images in the specified folder."""
    extracted_texts = {}
    
    print(f"Starting text extraction from images in: {folder_path}")
    
    # Loop through each file in the folder
    for filename in sorted(os.listdir(folder_path)):
        if filename.endswith(".PNG") or filename.endswith(".png"):
            print(f"Extracting text from: {filename}")
            file_path = os.path.join(folder_path, filename)
            extracted_texts[filename] = extract_text_from_image(file_path)

    print(f"Finished extracting text from images in: {folder_path}")
    return extracted_texts

def save_to_text_file(texts, output_filename="output.txt"):
    """Save the extracted texts to a text file."""
    with open(output_filename, 'w') as f:
        for filename, text in texts.items():
            f.write(f"Text from {filename}:\n")
            f.write(text + "\n\n")

    print(f"Saved extracted texts to: {output_filename}")

# Example usage
folder_path = "E:/tempImages"
texts = extract_text_from_folder(folder_path)
output_path = "extracted_texts.txt"
save_to_text_file(texts, output_path)
print(f"You can find the extracted texts in: {os.path.abspath(output_path)}")

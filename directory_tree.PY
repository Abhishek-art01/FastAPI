import os

def generate_tree(start_path, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        for root, dirs, files in os.walk(start_path):
            level = root.replace(start_path, "").count(os.sep)
            indent = "    " * level
            f.write(f"{indent}{os.path.basename(root)}/\n")
            subindent = "    " * (level + 1)
            for file in files:
                f.write(f"{subindent}{file}\n")

if __name__ == "__main__":
    start_directory = os.getcwd()   # current working directory
    output_txt = "directory_tree.txt"
    
    generate_tree(start_directory, output_txt)
    print("Directory tree saved to", output_txt)

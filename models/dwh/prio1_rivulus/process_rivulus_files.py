import os


def rename_and_modify(directory):
    for filename in os.listdir(directory):
        print('processing ', filename)
        if '.' in filename:
            # Split the filename into two parts based on the period
            parts = filename.split('.')

            # Get the new file name (second part after the period)
            new_filename = parts[1]+'.sql'

            # Read the content of the original file
            with open(os.path.join(directory, filename), 'r') as file:
                content = file.read()

            # Create the string to insert at the top of the file
            insert_string = f"{{{{ config(schema=var('{parts[0]}')) }}}}\n\n"

            # Insert the string at the beginning of the content
            modified_content = insert_string + content

            # Write the modified content back to the file with the new name
            with open(os.path.join(directory, new_filename), 'w') as new_file:
                new_file.write(modified_content)

            # Remove the original file
            os.remove(os.path.join(directory, filename))


def main():
    directory = '/Users/zaher.wanli/repos/dbt_poc/models/dwh/prio1_test/'  # Replace this with your directory path
    rename_and_modify(directory)


if __name__ == "__main__":
    main()

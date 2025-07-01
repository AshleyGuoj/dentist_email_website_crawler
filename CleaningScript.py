import pandas as pd
import re
import os

file_path = 'dentist_email_results_final_version.xlsx'
if not os.path.exists(file_path):
    print(f"File '{file_path}' not found. Please check the path and try again.")
    exit()

df = pd.read_excel(file_path)
df.insert(0, 'index', range(1, len(df) + 1))

# Define common dummy email patterns
dummy_patterns = [
    r'^info@',
    r'^user',
    r'^admin',
    r'^root',
    r'^test',
    r'^testing',
    r'^noreply@',
    r'^no-reply@',
    r'^donotreply@',
    r'^auto@',
    r'^nobody@',
    r'^email@',
    r'example',
    r'placeholder',
    r'fake',
    r'invalid',
    r'temp',
    r'^mail@',
    r'domain'
]

def is_dummy_email(email):
    if pd.isna(email):
        return False
    email = str(email).lower()
    return any(re.search(pat, email) for pat in dummy_patterns)

email_col = 'Email'
if email_col not in df.columns:
    print(f"Email column '{email_col}' not found. Available columns: {df.columns.tolist()}")
else:
    dummy_count = df[email_col].apply(is_dummy_email).sum()
    print(f"Identified {dummy_count} likely dummy email(s).")

    # Clear dummy emails
    df.loc[df[email_col].apply(is_dummy_email), email_col] = ''

    total_rows = len(df)
    print(f"Total records: {total_rows}")

    # Count valid emails
    valid_emails = df[email_col].notna() & (df[email_col].str.strip() != '')
    valid_count = valid_emails.sum()
    print(f"Valid emails after cleaning: {valid_count} ({valid_count / total_rows:.2%})")

    # Save cleaned data
    output_path = 'dentist_email_results_cleaned_final_version.xlsx'
    df.to_excel(output_path, index=False)
    print(f"Cleaned data saved to: {output_path}")


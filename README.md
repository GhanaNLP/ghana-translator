# Nkrane

Machine Translation enhanced with terminolgy controlled translation for Ghanaian languages.

**How It Works**

1. Identifies nouns and noun phrases in your input text.
2. Checks if they exist in your translated terminologies csv file.
3. Substitutes all matched nouns with unique placeholders.
4. Sends the text with placeholders to Google Translate for translation.
5. Replaces the placeholders in the translated text with your defined translations in your csv file.

## Supported Languages

The supported source language:
- `en` - English

The supported target languages:
- `ak` - Akan/Twi
- `ee` - Ewe
- `gaa` - Ga

## Installation

```bash
# Clone the repository
git clone https://github.com/ghananlp/nkrane.git
cd nkrane

# Install the package
pip install -e .
```

This will automatically:
- Install all dependencies (requests, spacy)
- Download the spaCy English model
- Set up the `nkrane-translate` command

## Quick Start

### 1. Create Your Terminology CSV

```csv
term,translation
house,efie
car,kaa
buy,tɔ
want,pɛ
```

### 2. Translate

**Command Line:**
```bash
nkrane-translate "I want to buy a house" -t ak -c my_terms.csv
```

**Python:**
```python
from nkrane_gt import NkraneTranslator

translator = NkraneTranslator(target_lang='ak', terminology_source='my_terms.csv')
result = translator.translate("I want to buy a house")
print(result['text'])  # Mepɛ sɛ metɔ efie
```

## Command Line Usage

### Basic Commands

```bash
# Translate text
nkrane-translate "TEXT" -t TARGET_LANG -c TERMS.csv

# Translate from file
nkrane-translate -f input.txt -t TARGET_LANG -c TERMS.csv -o output.txt

# Debug mode (show substitutions)
nkrane-translate "TEXT" -t TARGET_LANG -c TERMS.csv --debug

# Without terminology (direct Google Translate)
nkrane-translate "TEXT" -t TARGET_LANG

# Quiet mode (only output translation)
nkrane-translate "TEXT" -t TARGET_LANG -c TERMS.csv -q
```

### Arguments

| Argument | Description | Required |
|----------|-------------|----------|
| `text` or `-f FILE` | Text to translate or input file | Yes |
| `-t LANG` | Target language (e.g., ak, ee, gaa) | Yes |
| `-s LANG` | Source language (default: en) | No |
| `-c FILE` | Terminology CSV file | No |
| `-o FILE` | Output file | No |
| `--debug` | Show term substitutions | No |
| `-q` | Quiet mode (only output translation) | No |

### Examples

```bash
# Basic translation
nkrane-translate "I want to buy a house" -t ak -c terms.csv

# See what terms were substituted
nkrane-translate "I want to buy a house and a car" -t ak -c terms.csv --debug

# Batch translate a file
nkrane-translate -f input.txt -t ak -c terms.csv -o output.txt

# Direct translation without terminology
nkrane-translate "Hello world" -t ak

# Just the translation output
nkrane-translate "I want a house" -t ak -c terms.csv -q
```

### Debug Mode Output

```bash
$ nkrane-translate "I want to buy a house" -t ak -c terms.csv --debug

============================================================
🔍 DEBUG MODE
============================================================

Original text:
   I want to buy a house

Preprocessed text (with placeholders):
   I <0> to <1> <2>

Term substitutions (3):
   <0> → 'pɛ' (was: 'want')
   <1> → 'tɔ' (was: 'buy')
   <2> → 'efie' (was: 'house')

Google translation (with placeholders):
   Mepɛ sɛ metɔ efie

Final translation:
   Mepɛ sɛ metɔ efie

Translation time: 0.85s
============================================================
```

## Python API

### Basic Usage

```python
from nkrane_gt import NkraneTranslator

# Initialize translator
translator = NkraneTranslator(
    target_lang='ak',
    src_lang='en',                        # optional, default: 'en'
    terminology_source='my_terms.csv'     # optional
)

# Translate
result = translator.translate("I want to buy a house")
print(result['text'])
```

### With Debug Mode

```python
result = translator.translate("I want to buy a house", debug=True)

# Access result details
print(result['text'])                   # Final translation
print(result['original'])               # Original text
print(result['replacements_count'])     # Number of terms substituted
print(result['replaced_terms'])         # List of placeholders
print(result['translation_time'])       # Time in seconds
```

### Batch Translation

```python
texts = [
    "I want to buy a house.",
    "The school is near the market.",
    "We need water."
]

results = translator.batch_translate(texts, debug=True)

for result in results:
    print(result['text'])
```

### Without Terminology

```python
# Direct Google Translate
translator = NkraneTranslator(target_lang='ak')
result = translator.translate("Hello world")
print(result['text'])
```

## CSV Format

Your CSV must have at least 2 columns. Column names are auto-detected:

**Supported column names:**
- English: `term`, `text`, `english`, `source`, `word`
- Translation: `translation`, `text_translated`, `target`, `translated`

**Examples:**

```csv
term,translation
house,efie
car,kaa
```

```csv
english,twi
house,efie
car,kaa
```

```csv
text,text_translated
house,efie
car,kaa
```

All formats work the same.

## Result Dictionary

```python
{
    'text': str,                  # Final translated text
    'original': str,              # Original input
    'preprocessed': str,          # Text with placeholders
    'google_translation': str,    # Google output with placeholders
    'replacements_count': int,    # Number of terms substituted
    'replaced_terms': list,       # Placeholder IDs
    'src': str,                   # Source language code
    'dest': str,                  # Target language code
    'translation_time': float     # Seconds
}
```

## Troubleshooting

**Terms not being substituted:**
- Use `--debug` to see what's happening
- Check CSV format and spelling
- Matching is case-insensitive

**Translation timeout:**
- Default timeout is 30 seconds
- Check your internet connection

**spaCy model error (rare):**
If the automatic download failed during installation, run manually:
```bash
python -m spacy download en_core_web_sm
```

## License

MIT

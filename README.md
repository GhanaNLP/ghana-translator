# Nkrane: Enhanced Machine Translation with Terminology Control

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Nkrane - Google Translate (nkrane-gt) is a Python library that enhances the googletrans python library with terminology control for low-resource languages, with a focus on Ghanaian languages.

It solves the problem of inconsistent translations for critical terms by allowing you to enforce specific translations for nouns and noun phrases while letting Google Translate handle the grammatical structure.

---

## 🌍 Why Nkrane?

Standard machine translation often struggles with:
- **Inconsistent terminology** - The same word translated differently in different contexts
- **Named entities** - People names, place names, cultural terms mistranslated
- **Domain-specific vocabulary** - Technical, medical, or legal terms poorly handled
- **Low-resource languages** - Limited training data for African languages

**Nkrane solves this by:**
1. Extracting noun phrases from source text using NLP (spaCy)
2. Matching them against your terminology dictionary
3. Replacing content words with placeholders
4. Translating with Google Translate (grammar + stopwords)
5. Restoring your terminology with proper case preservation

---

## 📦 Installation

### From Source

```bash
git clone https://github.com/ghananlp/nkrane-gt.git
cd nkrane-gt
pip install -e .
```

### Requirements

```bash
pip install pandas spacy requests
python -m spacy download en_core_web_sm
```

---

## 🎯 Quick Start

### Basic Translation

```python
from nkrane_gt import NkraneTranslator

# Initialize with built-in Akan (Twi) dictionary
translator = NkraneTranslator(target_lang='ak')

# Translate
result = translator.translate("I want to buy a house and a car.")
print(result['text'])
# Output: "Me pɛ sɛ metɔ efie ne kar."
INFO:nkrane_gt.translator:Terminology loaded: 446281 total terms (446281 built-in, 0 user)

Mepɛ sɛ metɔ efie ne kaa.
{
  "text": "Mepɛ sɛ metɔ efie ne kaa.",
  "src": "en",
  "dest": "ak",
  "original": "I want to buy a house and a car.",
  "preprocessed": "I want to buy a <2> and a <1>.",
  "google_translation": "Mepɛ sɛ metɔ <2> ne <1>.",
  "replacements_count": 2,
  "src_google": "en",
  "dest_google": "ak",
  "replaced_terms": [
    "<1>",
    "<2>"
  ],
  "translation_time": 0.6670994758605957
}
```

### With Custom Terminology

```python
# Create custom CSV
cat > my_terms.csv << EOF
text,translation
house,ofie
car,ntentan
school,sukuu
EOF

# Use custom + built-in dictionary
translator = NkraneTranslator(
    target_lang='ak',
    terminology_source='my_terms.csv'
)

result = translator.translate("I want to buy a house.")
print(result['text'])
```

### Batch Translation

```python
texts = [
    "Buy a house today.",
    "The car is fast.",
    "Go to school."
]

results = translator.batch_translate(texts)
for r in results:
    print(f"{r['original']} -> {r['text']}")
```

---

## 🔧 Supported Languages

### Target Languages (Built-in Dictionaries)

| Code | Language | Terms Available |
|------|----------|----------------|
| `ak` | Akan (Twi) | 400,000+ |

### Source Languages

Any language supported by Google Translate (English, French, Spanish, etc.)

---

## 📚 How It Works

### The Translation Pipeline

```
Input: "I want to buy a house."
         ↓
1. Noun Phrase Extraction (spaCy)
   → Finds: "I" (pronoun), "a house" (noun chunk)
   → Filters stopwords: "a house" → "house"
   → Skips pronouns: "I" ignored
         ↓
2. Dictionary Matching
   → "house" in dictionary? ✓ → "efie"
         ↓
3. Preprocessing
   → "I want to buy <1>."
         ↓
4. Google Translate
   → "Me pɛ sɛ metɔ <1>."
         ↓
5. Postprocessing (case-matched)
   → "Me pɛ sɛ metɔ efie."
         ↓
Output: "Me pɛ sɛ metɔ efie."
```

## 🛠️ Advanced Usage

### CLI Commands

```bash
# Translate text
nkrane-gt translate "Hello world" --target ak

# List available terminology
nkrane-gt list

# Export terminology to JSON
nkrane-gt export --terminology my_terms.csv --format json

# Create sample terminology file
nkrane-gt sample --output sample_terms.csv
```

### Custom Terminology Format

CSV with columns (auto-detected):
- `text` / `english` / `term` / `word` - Source term
- `translation` / `text_translated` / `target` - Target translation

Example:
```csv
text,translation
custom house,me ofie
big car,ntentan kɛse
```

### Without Built-in Dictionary

```python
# Use only your custom terms
translator = NkraneTranslator(
    target_lang='ak',
    terminology_source='my_terms.csv',
    use_builtin=False  # Skip built-in dictionary
)
```

---

### Project Structure

```
nkrane-gt/
├── nkrane_gt/
│   ├── __init__.py          # Package exports
│   ├── translator.py        # Main NkraneTranslator class
│   ├── terminology_manager.py  # Dictionary & NLP logic
│   ├── language_codes.py    # Language code mappings
│   ├── utils.py            # Helper functions
│   ├── cli.py              # Command-line interface
│   └── data/               # Built-in dictionaries
│       ├── nouns_ak.pkl    # Akan/Twi terms
│       ├── nouns_ee.pkl    # Ewe terms
│       └── nouns_gaa.pkl   # Ga terms
├── tests/
├── setup.py
└── README.md
```

---

## 📖 Citation

If you use Nkrane in your research, please cite:

```bibtex
@software{nkrane_gt,
  title={Nkrane: Enhanced Machine Translation with Terminology Control},
  author={GhanaNLP},
  year={2026},
  url={https://github.com/ghananlp/nkrane-gt}
}
```

---

## 🤝 Contributing

Contributions welcome! Areas of interest:
- Additional language support
- Improved noun phrase extraction
- Domain-specific terminology packs
- Performance optimizations

---

## 📝 License

MIT License - see [LICENSE](LICENSE) file.

---

## 🙏 Acknowledgments

- Built on [Google Translate](https://translate.google.com/) for base translation
- Uses [spaCy](https://spacy.io/) for NLP processing
- Inspired by the need for better African language translation tools

**"Nkrane"** means "termites" in Akan/Twi.

---

## 📧 Contact

- Issues: [GitHub Issues](https://github.com/yourusername/nkrane-gt/issues)
- Email: natural.language.processing.gh@gmail.com

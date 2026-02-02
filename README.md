# Nkrane

A Python package that extends Google Translate and other MT models with terminology control and noun phrase augmentation to enhance translation quality for low-resource languages.

## Installation

```bash
# Install from PyPI (when available)
pip install nkrane

# Install from source
git clone https://github.com/GhanaNLP/nkrane.git
cd nkrane
pip install -e .

# Install spaCy model (required for noun phrase detection)
python -m spacy download en_core_web_sm
```

## Quick Start

### Basic Usage

```python
from nkrane import NkraneTranslator

# Initialize translator with your terminology
translator = NkraneTranslator(
    target_lang='twi',
    src_lang='en',
    terminology_source='my_terminology.csv'  # Optional
)

# Translate text
result = translator.translate("I need to buy water from the market.")
print(result['text'])
# Output: "Mehia sɛ mekotɔ nsu firi gyinabea no mu."
```

### Creating Terminology Files

Create a CSV file with the following columns:

```csv
id,term,translation,domain,language
1,house,ofie,general,en
2,car,ntentan,general,en
3,school,sukuu,education,en
4,water,nsu,general,en
5,market,gyinabea,commerce,en
```

### Sample Terminology

The package includes a sample terminology file. Create one with:

```python
from nkrane.utils import save_sample_terminology

save_sample_terminology('my_terminology.csv')
```

## Advanced Usage

### Batch Translation

```python
texts = [
    "I need water.",
    "The car is at the market.",
    "Children go to school."
]

translator = NkraneTranslator(target_lang='twi', terminology_source='terms.csv')
results = translator.batch_translate_sync(texts)

for result in results:
    print(result['text'])
```

### Using the CLI

```bash
# Translate text
nkrane translate "Hello world" --target twi --terminology my_terms.csv

# List available terminology options
nkrane list --terminology my_terms.csv

# Export terminology to JSON
nkrane export --terminology my_terms.csv --format json

# Create sample terminology
nkrane sample --output sample.csv
```

### Customizing Translation

```python
from nkrane import NkraneTranslator

translator = NkraneTranslator(
    target_lang='twi',
    src_lang='en',
    terminology_source='custom_terms.csv'
)

# Get detailed translation results
result = translator.translate("The red house has water.")
print(f"Translated: {result['text']}")
print(f"Original: {result['original']}")
print(f"Terms replaced: {result['replacements_count']}")
print(f"Replaced terms: {[t.term for t in result['replaced_terms']]}")
```

## Terminology Management

### CSV Format

Your terminology CSV should have the following structure:

```csv
id,term,translation,domain,language
1,computer,kɔmpiuta,technology,en
2,phone,foon,technology,en
3,hospital,ayaresabea,health,en
```

- **id**: Unique identifier (integer)
- **term**: English term (lowercase recommended)
- **translation**: Target language translation
- **domain**: Category (optional, default: "general")
- **language**: Source language (default: "en")

### Multiple Terminology Files

You can load multiple CSV files from a directory:

```python
translator = NkraneTranslator(
    target_lang='twi',
    terminology_source='terminology_folder/'  # Folder containing CSV files
)
```

## Language Support

Nkrane supports all languages available in Google Translate. For African languages, it uses appropriate language code conversions:

- English → Twi: `target_lang='twi'` or `target_lang='ak'`
- English → Hausa: `target_lang='hau'` or `target_lang='ha'`
- English → Yoruba: `target_lang='yor'` or `target_lang='yo'`

## Performance Tips

1. **Batch Processing**: Use `batch_translate_sync()` for multiple texts
2. **Terminology Size**: Keep terminology files focused on domain-specific terms
3. **Caching**: Implement your own caching for frequently translated phrases
4. **Network**: Ensure stable internet connection for Google Translate API calls

## Limitations

- Requires internet connection for Google Translate
- Noun phrase detection works best with English source text
- Terminology matches are case-insensitive but case-preserving
- Large terminology files may slow down preprocessing

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Citation

If you use Nkrane in your research, please cite:

```bibtex
@software{nkrane2026,
  title = {Nkrane: Enhanced Machine Translation with Terminology Control},
  author = {GhanaNLP Community},
  year = {2026},
  url = {https://github.com/GhanaNLP/nkrane}
}
```

## License

MIT License 

## Support

- Issues: [GitHub Issues](https://github.com/GhanaNLP/nkrane/issues)
- Discussions: [GitHub Discussions](https://github.com/GhanaNLP/nkrane/discussions)
- Email: natural.language.processing.gh@gmail.com

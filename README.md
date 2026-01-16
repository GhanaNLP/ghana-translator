# Nkrane Translator

A Python package that extends Google Translate and other MT models with terminology control and augmentation to enhance translation quality.  

## Features

- Supports using your own translations for specific vocabulary
- Translation to English, Twi, Ewe and other Ghanaian languages using augmented input to improve translation quality
- Simple terminology management with a single CSV file per language

## Installation

```bash
git clone https://github.com/GhanaNLP/ghana-translator.git 
cd ghana-translator
pip install -e .
```

Or install directly:

```bash
pip install git+https://github.com/GhanaNLP/ghana-translator.git 
```

## Quick Start

```python
import asyncio
from tc_translate import TCTranslator

async def main():
    translator = TCTranslator(target_lang='ak')  # 'ak' is the Google code for Twi
    result = await translator._translate_async("Please bring me some food from the kitchen.")
    print(result['text'])

# In Jupyter/Colab
await main()

# In regular Python
# asyncio.run(main())
```

## Terminology Files

Create terminology CSV files in the `tc_translate/terminologies/` folder with the naming convention:
`terminologies_{language}.csv`

CSV format:
```csv
id,term,translation
1,abattoir,aboa kum fie
2,aboiteau,nsu ban ɔkwan
3,kitchen,εbεso
...
```

### Example terminology files:
- `terminologies_twi.csv` - for Twi terminology
- `terminologies_ewe.csv` - for Ewe terminology  
- `terminologies_gaa.csv` - for Ga terminology

## Language Code Support

Ghana Translator supports both 3-letter (ISO 639-3) and 2-letter (ISO 639-1) language codes. The system automatically converts between them:

### Using original language names:
```python
# Your terminology file: terminologies_twi.csv
translator = TCTranslator(target_lang='twi')
```

### Using 2-letter Google codes:
```python
# Same terminology file, but using Google's code
translator = TCTranslator(target_lang='ak')  # 'ak' is Google's code for Akan/Twi
```


## Creating Terminology Files

1. Create a CSV file named `terminologies_{language}.csv` in your repository root
2. Include columns: `id`, `term`, `translation`
3. Add your terminology mappings
4. The translator will automatically use these terms during translation

Example `terminologies_twi.csv`:
```csv
id,term,translation
1,farm,farm
2,farmer,ɔbarima a ɔyɛ adwuma wɔ farm no so
3,abattoir,aboa kum fie
4,acreage,asase kɛse
```

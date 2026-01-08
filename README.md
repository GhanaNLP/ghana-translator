# Ghana Translator

A Python package that extends Google Translate with terminology control and augmentation to enhance translation quality.  

## Features

- Supports using your own translations for specific vocabulary
- Translation to english, twi and ewe using augmented input to imporve the quality of translation

## Installation

bash

```
git clone https://github.com/GhanaNLP/ghana-translator.git
cd ghana-translator
pip install -e .
```



Or install directly:

bash

```
pip install git+https://github.com/GhanaNLP/ghana-translator.git
```



## Quick Start

python

```
import asyncio
from tc_translate import TCTranslator

async def main():
    translator = TCTranslator(domain='agric', target_lang='ak') # or use target_lang='ee' for Ewe and target_lang='gaa' for Ga
    result = await translator._translate_async("The farmer uses an abattoir and acreage for farming.")
    print(result['text'])

# In Jupyter/Colab
await main()

# In regular Python
# asyncio.run(main())
```



## More Examples

For more comprehensive examples and usage patterns, see our [Google Colab Notebook](https://colab.research.google.com/drive/1xi6KXJXHB9F-zT-_6g23-YAUzDclyexP?usp=sharing).

The notebook includes:

- Side-by-side comparison of direct vs terminology-controlled translation
- Batch translation examples
- How to add your own terminology files
- Working with different domains and languages

## Terminology Files

Add your terminology CSV files in the `terminologies/` directory with naming convention:
`{domain}_terms_{language}.csv`

CSV format:

csv

```
id,term,translation
1,abattoir,aboa kum fie
2,aboiteau,nsu ban ɔkwan
...
```

## Language Code Support

TC Translate supports both 3-letter (ISO 639-3) and 2-letter (ISO 639-1)  language codes. The system automatically converts between them:

### Using 3-letter codes:

python

```
# Your terminology files: agric_terms_twi.csv
translator = TCTranslator(domain='agric', target_lang='twi')
```



### Using 2-letter Google codes:

python

```
# Same terminology file, but using Google's code
translator = TCTranslator(domain='agric', target_lang='ak')
```



### Terminology File Naming:

Name your terminology files using either format:

- `{domain}_terms_{3-letter-code}.csv` (e.g., `agric_terms_twi.csv`)
- `{domain}_terms_{2-letter-code}.csv` (e.g., `agric_terms_ak.csv`)

The system will automatically detect and convert between codes as needed.

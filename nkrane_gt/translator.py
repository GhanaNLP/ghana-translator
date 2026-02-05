# nkrane_gt/translator.py
import logging
import time
import requests
import urllib.parse
from typing import Dict, Any, Optional
from .terminology_manager import TerminologyManager
from .language_codes import convert_lang_code, is_google_supported

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NkraneTranslator:
    def __init__(self, target_lang: str, src_lang: str = 'en', 
                 terminology_source: str = None):
        """
        Initialize Nkrane Translator.

        Args:
            target_lang: Target language code (e.g., 'ak', 'ee', 'gaa')
            src_lang: Source language code (default: 'en')
            terminology_source: Path to user's terminology CSV file (optional)
        """
        self.target_lang = target_lang
        self.src_lang = src_lang

        # Initialize terminology manager (always with use_builtin=False)
        self.terminology_manager = TerminologyManager(
            target_lang=target_lang,
            user_csv_path=terminology_source,
            use_builtin=False
        )

        # Convert language codes to Google format
        self.src_lang_google = convert_lang_code(src_lang, to_google=True)
        self.target_lang_google = convert_lang_code(target_lang, to_google=True)

        # Check if Google Translate supports these languages
        if not is_google_supported(src_lang):
            logger.warning(f"Source language '{src_lang}' may not be supported by Google Translate")

        if not is_google_supported(target_lang):
            logger.warning(f"Target language '{target_lang}' may not be supported by Google Translate")

        # Log terminology stats
        stats = self.terminology_manager.get_terms_count()
        if stats['total'] > 0:
            logger.info(f"Terminology loaded: {stats['total']} terms from user CSV")
        else:
            logger.info("No terminology loaded - translations will use Google Translate directly")
        
        if use_pivot:
            logger.info(f"Using pivot translation: {src_lang} → {pivot_lang} → {target_lang}")

    def _google_translate_sync(self, text: str) -> str:
        """
        Synchronous Google Translate using requests.
        Uses the same endpoint that googletrans library uses.
        
        Args:
            text: Text to translate
            
        Returns:
            Translated text
        """
        # Google Translate web API endpoint (same one googletrans uses)
        url = "https://translate.googleapis.com/translate_a/single"

        params = {
            'client': 'gtx',
            'sl': self.src_lang_google,
            'tl': self.target_lang_google,
            'dt': 't',
            'q': text,
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            # Parse the response (Google returns a nested list)
            data = response.json()

            # Extract translated text from the nested structure
            # data[0] contains the translation segments
            translated_parts = []
            if data and len(data) > 0:
                for item in data[0]:
                    if item and len(item) > 0:
                        translated_parts.append(item[0])

            return ''.join(translated_parts)

        except requests.exceptions.Timeout:
            raise TimeoutError("Google Translate request timed out after 30 seconds")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Google Translate API error: {e}")
        except (IndexError, TypeError) as e:
            raise Exception(f"Failed to parse Google Translate response: {e}")

    def translate(self, text: str, **kwargs) -> Dict[str, Any]:
        """
        Translate text with terminology control.

        Args:
            text: Text to translate
            **kwargs: Additional arguments (kept for API compatibility)

        Returns:
            Dictionary with translation results
        """
        start_time = time.time()

        try:
            # Step 1: Preprocess - replace noun phrases with placeholders
            preprocessed_text, replacements, original_cases = self.terminology_manager.preprocess_text(text)

            logger.debug(f"Preprocessed text: {preprocessed_text}")
            logger.debug(f"Replacements: {list(replacements.keys())}")

            # Step 2: Translate using Google Translate
            logger.debug(f"Translating: {self.src_lang} → {self.target_lang}")
            translated_with_placeholders = self._google_translate_sync(preprocessed_text)

            # Step 3: Postprocess - replace placeholders with translations
            final_text = self.terminology_manager.postprocess_text(
                translated_with_placeholders,
                replacements,
                original_cases
            )

            end_time = time.time()

            return {
                'text': final_text,
                'src': self.src_lang,
                'dest': self.target_lang,
                'original': text,
                'preprocessed': preprocessed_text,
                'google_translation': translated_with_placeholders,
                'replacements_count': len(replacements),
                'src_google': self.src_lang_google,
                'dest_google': self.target_lang_google,
                'replaced_terms': list(replacements.keys()),
                'translation_time': end_time - start_time
            }

        except Exception as e:
            logger.error(f"Translation failed: {e}")
            raise

    def batch_translate(self, texts: list, **kwargs) -> list:
        """Translate multiple texts."""
        results = []
        for i, text in enumerate(texts):
            try:
                result = self.translate(text, **kwargs)
                results.append(result)

                # Add a small delay to avoid rate limiting
                if i < len(texts) - 1:
                    time.sleep(0.5)

            except Exception as e:
                logger.error(f"Failed to translate text {i}: {e}")
                results.append({
                    'text': '',
                    'error': str(e),
                    'original': text
                })

        return results

# nkrane/translator.py
import asyncio
import logging
from typing import Dict, Any, Optional

from googletrans import Translator as GoogleTranslator
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
            target_lang: Target language code (e.g., 'twi' or 'ak')
            src_lang: Source language code (default: 'en')
            terminology_source: Path to terminology CSV file or directory (optional)
        """
        self.target_lang = target_lang
        self.src_lang = src_lang
        
        # Initialize terminology manager
        self.terminology_manager = TerminologyManager(terminology_source)
        
        # Convert language codes to Google format
        self.src_lang_google = convert_lang_code(src_lang, to_google=True)
        self.target_lang_google = convert_lang_code(target_lang, to_google=True)
        
        # Check if Google Translate supports these languages
        if not is_google_supported(src_lang):
            logger.warning(f"Source language '{src_lang}' may not be supported by Google Translate")
        
        if not is_google_supported(target_lang):
            logger.warning(f"Target language '{target_lang}' may not be supported by Google Translate")
    
    async def _translate_async(self, text: str, **kwargs) -> Dict[str, Any]:
        """
        Internal async translation method.
        
        Args:
            text: Text to translate
            **kwargs: Additional arguments for Google Translate
            
        Returns:
            Dictionary with translation results
        """
        # Step 1: Preprocess - replace noun phrases with IDs
        preprocessed_text, replacements, original_cases = self.terminology_manager.preprocess_text(text)
        
        logger.debug(f"Preprocessed text: {preprocessed_text}")
        logger.debug(f"Replacements: {list(replacements.keys())}")
        
        # Step 2: Translate with Google Translate
        try:
            async with GoogleTranslator() as translator:
                google_result = await translator.translate(
                    preprocessed_text,
                    src=self.src_lang_google,
                    dest=self.target_lang_google,
                    **kwargs
                )
                
                translated_with_placeholders = google_result.text
                
                # Step 3: Postprocess - replace IDs with translations
                final_text = self.terminology_manager.postprocess_text(
                    translated_with_placeholders,
                    replacements,
                    original_cases
                )
                
                return {
                    'text': final_text,
                    'src': self.src_lang,
                    'dest': self.target_lang,
                    'original': text,
                    'preprocessed': preprocessed_text,
                    'google_translation': google_result.text,
                    'replacements_count': len(replacements),
                    'src_google': self.src_lang_google,
                    'dest_google': self.target_lang_google,
                    'replaced_terms': list(replacements.values())
                }
                
        except Exception as e:
            logger.error(f"Translation failed: {e}")
            raise
    
    def translate(self, text: str, **kwargs) -> Dict[str, Any]:
        """
        Translate text with terminology control.
        
        Args:
            text: Text to translate
            **kwargs: Additional arguments for Google Translate
            
        Returns:
            Dictionary with translation results
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._translate_async(text, **kwargs), 
                    loop
                )
                return future.result(timeout=30)
            else:
                return asyncio.run(self._translate_async(text, **kwargs))
        except RuntimeError:
            return asyncio.run(self._translate_async(text, **kwargs))
        except TimeoutError:
            logger.error("Translation timed out after 30 seconds")
            raise
    
    async def batch_translate(self, texts: list, **kwargs) -> list:
        """Translate multiple texts asynchronously."""
        results = []
        for text in texts:
            result = await self._translate_async(text, **kwargs)
            results.append(result)
            await asyncio.sleep(0.1)  # Rate limiting
        return results
    
    def batch_translate_sync(self, texts: list, **kwargs) -> list:
        """Synchronous wrapper for batch translation."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.batch_translate(texts, **kwargs), 
                    loop
                )
                return future.result(timeout=300)
            else:
                return asyncio.run(self.batch_translate(texts, **kwargs))
        except RuntimeError:
            return asyncio.run(self.batch_translate(texts, **kwargs))
        except TimeoutError:
            logger.error("Batch translation timed out after 5 minutes")
            raise

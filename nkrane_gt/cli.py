#!/usr/bin/env python3
"""
Nkrane-GT Command Line Interface
Simple translation tool with terminology control
"""

import argparse
import sys
from nkrane_gt import NkraneTranslator

def main():
    parser = argparse.ArgumentParser(
        description='Nkrane-GT: Enhanced Machine Translation with Terminology Control',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Translate with terminology CSV
  python run.py "I want to buy a house" -t ak -c my_terms.csv
  
  # Translate without terminology (direct translation)
  python run.py "Hello world" -t ak
  
  # Enable debug mode to see substitutions
  python run.py "I want to buy a house and a car" -t ak -c my_terms.csv --debug
  
  # Translate from a different source language
  python run.py "Hola mundo" -s es -t en
  
  # Batch translate from file
  python run.py -f input.txt -t ak -c my_terms.csv -o output.txt

Supported target languages:
  ak   - Akan/Twi
  ee   - Ewe
  gaa  - Ga
  en   - English
  es   - Spanish
  fr   - French
  ... and many more (see Google Translate supported languages)
        """
    )
    
    # Input arguments
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        'text',
        nargs='?',
        help='Text to translate'
    )
    input_group.add_argument(
        '-f', '--file',
        help='Input file with text to translate (one sentence per line)'
    )
    
    # Language arguments
    parser.add_argument(
        '-t', '--target',
        required=True,
        help='Target language code (e.g., ak, ee, gaa)'
    )
    parser.add_argument(
        '-s', '--source',
        default='en',
        help='Source language code (default: en)'
    )
    
    # Terminology arguments
    parser.add_argument(
        '-c', '--csv',
        '--terminology',
        dest='terminology',
        help='Path to terminology CSV file (optional)'
    )
    
    # Output arguments
    parser.add_argument(
        '-o', '--output',
        help='Output file path (optional, defaults to stdout)'
    )
    
    # Debug mode
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode to see term substitutions'
    )
    
    # Quiet mode
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Quiet mode - only output translation (suppress info messages)'
    )
    
    args = parser.parse_args()
    
    # Suppress logging if quiet mode
    if args.quiet:
        import logging
        logging.getLogger().setLevel(logging.ERROR)
    
    try:
        # Initialize translator
        if not args.quiet:
            print(f"🚀 Initializing translator ({args.source} → {args.target})...")
        
        translator = NkraneTranslator(
            target_lang=args.target,
            src_lang=args.source,
            terminology_source=args.terminology
        )
        
        # Get text to translate
        if args.file:
            # Read from file
            with open(args.file, 'r', encoding='utf-8') as f:
                texts = [line.strip() for line in f if line.strip()]
            
            if not args.quiet:
                print(f"📄 Loaded {len(texts)} lines from {args.file}")
            
            # Batch translate
            results = translator.batch_translate(texts, debug=args.debug)
            
            # Prepare output
            output_lines = []
            for i, result in enumerate(results):
                if 'error' in result:
                    output_lines.append(f"[ERROR] {result['error']}")
                else:
                    output_lines.append(result['text'])
                    if args.debug and not args.quiet:
                        print(f"\n[{i+1}/{len(texts)}] {result['original']}")
                        print(f"    → {result['text']}")
                        print(f"    Terms replaced: {result['replacements_count']}")
            
            output_text = '\n'.join(output_lines)
            
        else:
            # Single translation
            result = translator.translate(args.text, debug=args.debug)
            
            if 'error' in result:
                print(f"❌ Error: {result['error']}", file=sys.stderr)
                sys.exit(1)
            
            output_text = result['text']
            
            # Print summary if not in quiet mode
            if not args.quiet and not args.debug:
                print(f"\n📝 Original: {args.text}")
                print(f"✅ Translation: {output_text}")
                if result['replacements_count'] > 0:
                    print(f"📋 Terms replaced: {result['replacements_count']}")
        
        # Output to file or stdout
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output_text)
            if not args.quiet:
                print(f"\n💾 Translation saved to {args.output}")
        else:
            if args.quiet:
                # In quiet mode, just print the translation
                print(output_text)
            elif not args.debug:
                # Already printed above
                pass
        
        if not args.quiet:
            print("\n✨ Done!")
        
    except FileNotFoundError as e:
        print(f"❌ Error: File not found - {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Translation interrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

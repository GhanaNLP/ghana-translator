# nkrane/utils.py
import json
import pandas as pd
from typing import Dict, List
from .terminology_manager import TerminologyManager

def list_available_options(terminology_source: str = None) -> Dict:
    """
    List available domains and languages from terminology.
    
    Args:
        terminology_source: Path to terminology CSV file or directory
        
    Returns:
        Dictionary with available options
    """
    manager = TerminologyManager(terminology_source)
    terms = manager.get_terms()
    
    # Extract unique domains and languages
    domains = set()
    languages = set()
    
    for term_obj in terms.values():
        domains.add(term_obj.domain)
        languages.add(term_obj.language)
    
    return {
        'domains': sorted(list(domains)),
        'languages': sorted(list(languages)),
        'term_count': len(terms)
    }

def export_terminology(terminology_source: str = None, 
                      output_format: str = 'json',
                      domain: str = None) -> str:
    """
    Export terminology to various formats.
    
    Args:
        terminology_source: Path to terminology CSV file or directory
        output_format: 'json', 'csv', or 'dict'
        domain: Filter by domain (optional)
        
    Returns:
        Terminology in requested format
    """
    manager = TerminologyManager(terminology_source)
    terms = manager.get_terms()
    
    # Filter by domain if specified
    if domain:
        terms = {k: v for k, v in terms.items() if v.domain == domain}
    
    # Convert to list of dictionaries
    terms_list = [
        {
            'id': term.id,
            'term': term.term,
            'translation': term.translation,
            'domain': term.domain,
            'language': term.language
        }
        for term in terms.values()
    ]
    
    if output_format == 'json':
        return json.dumps(terms_list, indent=2, ensure_ascii=False)
    elif output_format == 'csv':
        import io
        import csv
        
        output = io.StringIO()
        if terms_list:
            writer = csv.DictWriter(output, fieldnames=terms_list[0].keys())
            writer.writeheader()
            writer.writerows(terms_list)
        return output.getvalue()
    else:  # 'dict'
        return terms_list

def create_sample_terminology() -> pd.DataFrame:
    """
    Create a sample terminology DataFrame for testing.
    
    Returns:
        Sample terminology as DataFrame
    """
    data = {
        'id': [1, 2, 3, 4, 5],
        'term': ['house', 'car', 'school', 'water', 'market'],
        'translation': ['ofie', 'ntentan', 'sukuu', 'nsu', 'gyinabea'],
        'domain': ['general', 'general', 'education', 'general', 'commerce'],
        'language': ['en', 'en', 'en', 'en', 'en']
    }
    
    return pd.DataFrame(data)

def save_sample_terminology(filepath: str = 'sample_terminology.csv'):
    """
    Save sample terminology to a CSV file.
    
    Args:
        filepath: Path where to save the sample terminology
    """
    df = create_sample_terminology()
    df.to_csv(filepath, index=False)
    print(f"Sample terminology saved to {filepath}")

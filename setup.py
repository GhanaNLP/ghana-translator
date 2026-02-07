from setuptools import setup, find_packages
from setuptools.command.install import install
import subprocess
import sys
import os

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        # Download spaCy model after installation
        try:
            print("\n" + "="*60)
            print("Downloading spaCy English model...")
            print("="*60)
            subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
            print("\n✅ spaCy model downloaded successfully!")
            print("="*60 + "\n")
        except subprocess.CalledProcessError:
            print("\n⚠️  Warning: Failed to download spaCy model automatically.")
            print("Please run manually: python -m spacy download en_core_web_sm")
            print("="*60 + "\n")

# Read requirements from requirements.txt
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    with open(requirements_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

# Read README
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nkrane-gt",
    version="0.3.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Enhanced machine translation with terminology control using Google Translate",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/nkrane-gt",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Localization",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    cmdclass={
        'install': PostInstallCommand,
    },
    entry_points={
        "console_scripts": [
            "nkrane-translate=nkrane_gt.cli:main",
        ],
    },
    include_package_data=True,
)

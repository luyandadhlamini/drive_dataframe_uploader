from distutils.core import setup
version = '0.1.2'
setup(
  name = 'drive_dataframe_uploader',         
  packages = ['drive_dataframe_uploader'],   
  version = version,      
  license='MIT',        
  description = 'Package uses a dictionary to download, append or replace Google Sheets with a pandas dataframe.',   
  author = 'Luyanda Dhlamini',                   
  author_email = 'luyanda.dhlamini@gmail.com',  
  url = 'https://github.com/luyandadhlamini',   
  download_url = 'https://github.com/luyandadhlamini/drive_dataframe_uploader/archive/refs/tags/v{}.tar.gz'.format(version), 
  keywords = ['Python', 'Google Drive', 'drive', 'API', 'wrapper', 'spreadsheet', 'sheets' ],   
  install_requires=[            
          'pandas',
          'gspread',
          'gspread-dataframe'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      
    'Intended Audience :: Developers',      
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   
    'Programming Language :: Python :: 3',      
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
  ],
    
    long_description = 'Package uses a dictionary to download, append or replace Google Sheets with a pandas dataframe.',
    long_description_content_type = 'text/markdown'
)

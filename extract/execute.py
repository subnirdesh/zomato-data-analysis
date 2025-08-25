import os,sys,requests,json,time
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utility.utility import setup_logging,format_time

def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)    
        logger.debug(f"Downloaded zip file : {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")
    
def extract_zip_file(zip_filename, output_dir):

    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    
    logger.debug(f"Extracted files written to: {output_dir}")
    os.remove(zip_filename)



if __name__ == "__main__":


    logger=setup_logging("extract.log")
    start=time.time()


    if len(sys.argv) < 2:
        logger.debug("Extraction path is required")
        logger.debug(" Usage: python3 execute.py /nirdeshsubedi/Data/Extraction")
    else:
        try:
            logger.info("Starting Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/anas123siddiqui/zomato-database"
                            
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(zip_filename, EXTRACT_PATH)
        
            logger.info("Extraction Sucessfully Completed!!!")
        except Exception as e:
            logger.error(f"Error: {e}")

    end=time.time()
    logger.info(f"Total time taken{ format_time(end-start)}")


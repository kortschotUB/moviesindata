import os
import requests
from PIL import Image, ImageDraw
from io import BytesIO
import time
import cv2
from dotenv import load_dotenv
from core import exceptionOutput
load_dotenv()

TMDB_AUTH_TOKEN = os.getenv("TMDB_AUTH_TOKEN")



def getHeadshot(personId, headers, imagesToInclude = 1):
    url = f"https://api.themoviedb.org/3/person/{personId}/images"
    headshotPath = f'../data/headshots/{personId}'

    os.makedirs(headshotPath, exist_ok=True)
    
    if len(os.listdir(headshotPath)) > 0:
        return

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        time.sleep(2)
        getHeadshot(personId, headers, imagesToInclude)
    
    try:
        profiles = response.json()['profiles']
        paths = [p['file_path'] for p in profiles]

        for i, path in enumerate(paths[:imagesToInclude]):
            imgUrl = f"http://image.tmdb.org/t/p/w300/{path}"
            image_response = requests.get(imgUrl)

            if image_response.status_code == 200:
                image = Image.open(BytesIO(image_response.content))
                imagePath = os.path.join(headshotPath, f"{personId}_{i}.jpg")
                image.save(imagePath)
    except:
        print(response.json())
        return response.status_code

    return response.status_code

# Function to crop image in a circle
def cropCircle(imgPath, debug=False):
    try:

        # load image
        img = Image.open(imgPath)

        # crop image 
        width, height = img.size
        x = (width - height)//2
        imgCropped = img.crop((x, 0, x+height, height))

        # create grayscale image with white circle (255) on black background (0)
        mask = Image.new('L', imgCropped.size)
        maskDraw = ImageDraw.Draw(mask)
        width, height = imgCropped.size
        maskDraw.ellipse((0, 0, width, height), fill=255)
        #mask.show()

        # add mask as alpha channel
        imgCropped.putalpha(mask)

        # save as png which keeps alpha channel 
        imgCropped.save(imgPath.replace('.jpg','.png'))

        return 200
    except Exception as e:
        if debug:
            print(exceptionOutput(e))
        
        return 400

def extractFaces(imgPath, makePretty: bool = False, debug=False):
    try:# Read the input image 
        if not os.path.exists(imgPath):
            return 400
        
        img = cv2.imread(imgPath) 
        savePath = os.path.join('/'.join(imgPath.split('/')[:-1]), "00_faceExtracted.jpg")
        
        # Convert into grayscale 
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        face_cascade = cv2.CascadeClassifier('../assets/haarcascade_frontalface_alt2.xml') 
        # Detect faces 
        faces = face_cascade.detectMultiScale(gray, 1.1, 4)

        for (x, y, w, h) in faces: 
            # cv2.rectangle(img, (x, y), (x+w, y+h),  
            #             (0, 0, 255), 2) 
            
            faces = img[y:y + h, x:x + w] 
            cv2.imshow("face",faces) 
            cv2.imwrite(savePath, faces) 

        if len(faces) == 0 and debug:
            print(f"ERROR: NO FACES DETECTED FOR {imgPath}")
        # Crop to circle
        if not makePretty:
            return 200

        cropCircle(savePath)

        return 200
    except Exception as e:
        if debug:
            print(exceptionOutput(e))
        
        return 400
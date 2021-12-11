from WriteToSheet import *
import schedule
import time

class Update:
    def __init__(self):
        self.toSheet = Integration()
        self.checkToAdd = self.toSheet.checkToAdd()
        self.checkToDelete = self.toSheet.checkToDelete()

    def UpdateSheet(self):
        self.checkToAdd
        self.checkToDelete
while 1:
    try:
        update = Update()
        update.UpdateSheet()
        print("Checking")
    except:
        pass
    time.sleep(3)
from telethon import TelegramClient, events, sync
import nest_asyncio
import pandas as pd
from datetime import datetime
import schedule
import asyncio
import os

nest_asyncio.apply()

folder_path = "./Tele_data/"

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

#create getting message function
class get_data:
    def __init__(self, channel_id, channel_name):
        self.channel_id = channel_id
        self.channel_name = channel_name
        self.df = pd.DataFrame(columns=['time', 'channel', 'text', 'media'])

    async def get_message(self, event):
        message_data = {
            'time': event.date,
            'channel': self.channel_id,
            'text': event.message.message,
            'media': bool(event.message.media)
        }
        new_row = pd.DataFrame([message_data])
        self.df = pd.concat([self.df, new_row], ignore_index=True)

api_id = 'api_id'
api_hash = 'api_hash'
#ningad1, ID: 2116583573, for test
#bbcrussian, ID: 1003921752
#rbc_news, ID: 1099860397
#varlamov_news, ID: 1124038902
#telegram, ID: 1005640892
channel_names = ['BBCRussian', 'RBCNews', 'VarlamovNews', 'TelegramNews', 'NingaD1']
channel_ids = ['-1001003921752', '-1001099860397', '-1001124038902', '-1001005640892','-1002116583573']
client = TelegramClient('tele_data', api_id, api_hash)

#Traverse
channel_info_list = [get_data(int(single_channel_id), single_channel_name) for single_channel_id, single_channel_name in zip(channel_ids, channel_names)]

#Telegram Api for getting channel message
@client.on(events.NewMessage(chats=[single_id.channel_id for single_id in channel_info_list]))
async def push_message_df(event):
    for single_channel in channel_info_list:
        if event.chat_id == single_channel.channel_id:
            await single_channel.get_message(event)
            break

#save as paquet
def save_data():
    for single_channel in channel_info_list:
        if not single_channel.df.empty:
            file_name = f'{folder_path}{single_channel.channel_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet'
            single_channel.df.to_parquet(file_name)
            single_channel.df = single_channel.df.iloc[0:0]

#1 time/1s
schedule.every(1).minutes.do(save_data)

async def main():
    await client.start()
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

asyncio.run(main())
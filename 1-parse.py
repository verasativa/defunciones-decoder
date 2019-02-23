
# coding: utf-8

# In[1]:


import os, time, datetime, dataDecode, json
import pandas as pd
from tqdm.auto import tqdm
tqdm.pandas()
import importlib
from hurry.filesize import size

# Locals v/s floydhub
if 'TERM_PROGRAM' in os.environ:
    base_path = '../datasets/defunciones-deis/'
    save_path = './output/'
else:
    base_path = '/floyd/input/defunciones_deis/'
    save_path = './output/'



with open('data/dtypes.json') as json_data:
    read_dtypes = json.load(json_data)
    
df = pd.read_csv('data/defunciones-deis-1998-2016.csv', dtype=read_dtypes)


# In[2]:


#df = df.sample(frac=.05)
#df.shape


# In[ ]:


importlib.reload(dataDecode)
decoder = dataDecode.decoder(base_path + '_ref/', debug=True)

pdf = df.progress_apply(decoder.decode___row, axis=1)


# In[ ]:


base_name = 'defunciones-deis-1998-2016-parsed-{}'.format(size(len(df)).replace('B', ''))


# In[ ]:


pdf.to_csv(save_path + base_name + '.csv', index=False)
pdf.to_excel(save_path + base_name + '.xlsx', index=False)


# In[ ]:


# Dtypes
dtypes = {}

for key in pdf.dtypes.to_dict().keys():
    dtypes[key] = str(pdf.dtypes[key])

with open(save_path + 'dtypes.json', 'w') as outfile:
    json.dump(dtypes, outfile)


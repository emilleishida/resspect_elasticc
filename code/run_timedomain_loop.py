# Copyright 2023 resspect software
# Author: Emille E. O. Ishida
#
# created on 28 February 2023
#
# Licensed MIT License;
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://opensource.org/license/mit/
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from resspect import time_domain_loop
import pandas as pd
import os

####################################################################################
#######################  User choices  #############################################

###### For TOM integration
    
days = [42, 50]             ## days you want to perform the query
batch = 1                   # number of objects to be queried per day. For a 
                            # taking into account telescope resources choose None
                            # this will query a different number of objects each day

training = 20               # number of points in initial training
strategy = 'UncSampling'    # learning strategy

# save query to file
output_query_file_elasticc = '/media/RESSPECT/data/PLAsTiCC/ELAsTiCC_ids/queried/' + '/batch' + str(batch) + \
                     '/' + strategy + '/queried_elasticc_ids_' + strategy + '_' + str(training) + '_batch' + str(batch) + '.csv'

##################################################################################


sep_files = True
save_full_query= False

# folder where queries will be stored
output_query_file = '/media/RESSPECT/data/PLAsTiCC/ELAsTiCC_ids/queried/batch' + str(batch) + '/' + \
                    strategy + '/queried_' + strategy + '_' + str(training) + '_batch' + str(batch) + '.csv'

output_diag_file = '/media/RESSPECT/data/PLAsTiCC/ELAsTiCC_ids/metrics/' + \
                    'metrics_' + strategy + '_' + str(training) + '_batch' + str(batch) + '.csv'

path_to_features_dir = '/media/RESSPECT/data/PLAsTiCC/for_pipeline/DDF/features/pool/'
  
if batch == None:
    budgets = (2. * 3600, 1. * 3600)
else:
    budgets = None

classifier = 'RandomForest'
n_estimators = 1000         # number of trees in the random forest algorithm
clf_bootstrap = False #True
feature_method = 'Bazin'
screen = True
fname_pattern = ['day_', '.csv']
canonical = False
queryable= True
    
path_to_ini_files = {}
path_to_ini_files['train'] = '/media/RESSPECT/data/PLAsTiCC/for_pipeline/DDF/features/PLAsTiCC_Bazin_train.csv'
path_to_ini_files['test'] = '/media/RESSPECT/data/PLAsTiCC/for_pipeline/DDF/features/PLAsTiCC_Bazin_test.csv'
path_to_ini_files['validation'] = '/media/RESSPECT/data/PLAsTiCC/for_pipeline/DDF/features/PLAsTiCC_Bazin_validation.csv'

survey='LSST'
    
# run time domain loop
time_domain_loop(days=days, output_metrics_file=output_diag_file,
                 output_queried_file=output_query_file,
                 path_to_features_dir=path_to_features_dir,
                 budgets=budgets, clf_bootstrap=clf_bootstrap,
                 strategy=strategy, fname_pattern=fname_pattern, batch=batch, classifier=classifier,
                 canonical=canonical, sep_files=sep_files,
                 screen=screen, initial_training=training, path_to_ini_files=path_to_ini_files,
                 survey=survey, queryable=queryable, n_estimators=n_estimators, save_full_query=save_full_query)


# substitute elasticc ids
ids_map = pd.read_csv('/media/RESSPECT/data/PLAsTiCC/ELAsTiCC_ids/plasticc_elasticc_map.csv', index_col=False)

features = pd.read_csv(output_query_file, index_col=False)

new_id_set = []

for i in range(features.shape[0]):
    
    old_id = features.iloc[i]['id']
    new_id = ids_map[ids_map['id'].values == old_id]['diaObjectId'].values[0]
    
    features.at[i, 'id'] = new_id

features = features.rename(columns={'id':'diaObjectId'})

features.to_csv(output_query_file_elasticc, index=False)
os.remove(output_query_file)
    
    
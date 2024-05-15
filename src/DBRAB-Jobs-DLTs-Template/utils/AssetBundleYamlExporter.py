import databricks.sdk as sdk
import yaml
import os 
import glob


class JobDLTABYamlExporter:
    def __init__(self,
                 dbr_host,
                 dbr_access_token,
                 new_object_suffix ="-[UC]"):
        self.__new_object_suffix = new_object_suffix
        self.__jobs_folder =  os.getcwd().replace("utils",f"resources\\Jobs")
        self.__dlts_folder =  os.getcwd().replace("utils",f"resources\\DLT")
        self.__pipelines_ids = dict()
        self.__dbr_workspace = sdk.WorkspaceClient(host=dbr_host, token= dbr_access_token)
        self.__catalog_by_storage_mapping = None

    def version(self):
        print("Version:0.0.5")

    def convert_DLT_to_UC(self,catalog_by_storage):
        self.__catalog_by_storage_mapping = catalog_by_storage

    def delete_Jobs(self, job_name_suffix=None):
        if job_name_suffix is None:
            job_name_suffix = self.__new_object_suffix
        for job in self.__dbr_workspace.jobs.list():
            if job.as_dict()['settings']['name'].endswith(job_name_suffix):
                print(f"Deleting Job:{job.as_dict()['settings']['name']}")
                self.__dbr_workspace.jobs.delete(job.job_id)

    def delete_DLTs(self, dlt_pipeline_name_suffix=None):
        if dlt_pipeline_name_suffix is None:
            dlt_pipeline_name_suffix = self.__new_object_suffix
        for dlt in self.__dbr_workspace.pipelines.list_pipelines():
            if dlt.as_dict()['name'].endswith(dlt_pipeline_name_suffix):
                print(f"Deleting DLT: {dlt.as_dict()['name']})")
                self.__dbr_workspace.pipelines.delete(dlt.pipeline_id)

    def export(self):
        self.export_DLts()
        self.export_Jobs()

    def export_DLts(self):
        print("Exporting DLTs...")
        self.__clean_DLTs()
        self.__pipelines_ids = dict()
        dlt_pipelines = self.__list_DLTs()
        for pipeline_info in dlt_pipelines:
            print(f"Exporting DLT pipeline :{pipeline_info['spec']['name']}")
            if self.__catalog_by_storage_mapping is not None:
                print("Converting to UC....")
                if "storage" in pipeline_info['spec']: 
                    dlt_storage = str(pipeline_info['spec']['storage'])
                    pipeline_info['spec']['channel'] = 'PREVIEW'
                    for storage in self.__catalog_by_storage_mapping:
                        if dlt_storage.lower().startswith(storage.lower()):
                            pipeline_info['spec']['catalog'] = "${bundle.target}_"+self.__catalog_by_storage_mapping[storage]
                    pipeline_info['spec'].pop('storage')
                    if 'configuration' not in pipeline_info['spec']:
                         pipeline_info['spec']['configuration'] =dict()
                    pipeline_info['spec']['configuration']['spark.databricks.sql.initial.catalog.name']='hive_metastore'

            pipeline_id = pipeline_info['spec'].pop('id')
            dlt_name = f"{pipeline_info['spec']['name']}{self.__new_object_suffix}"
            dlt_name_yaml ="DLT-"+dlt_name.replace("[","").replace("]","")
            pipeline_info['spec']['name'] = dlt_name
            
            self.__pipelines_ids[pipeline_id] = dlt_name_yaml
            resources = dict()
            pipelines = dict()
            dlt = dict()
            dlt[dlt_name_yaml]=pipeline_info['spec']
            pipelines['pipelines'] = dlt
            resources['resources'] = pipelines
            ymal_payload = yaml.dump(resources)
            self.__dump_DLT(dlt_name_yaml,ymal_payload)

    def export_Jobs(self):
        print("Exporting Jobs...")
        self.__clean_Jobs()
        wjobs = self.__list_Jobs()
        for j_info in wjobs:
            print(f"Exporting Job:{j_info['settings']['name']}")
            for task in j_info['settings'].get('tasks'):
                if task is not None and task.get('pipeline_task') is not None:
                    #MAP ID->New PIPELINE NAME
                    pipeline_id = task.get('pipeline_task')['pipeline_id']
                    dlt_pipeline_conf_name = self.__pipelines_ids.get(pipeline_id)
                    if dlt_pipeline_conf_name is not None:
                        task.get('pipeline_task')['pipeline_id']='${resources.pipelines.'+dlt_pipeline_conf_name+'.id}'
            job_name = f"{j_info['settings']['name']}{self.__new_object_suffix}"
            job_name_yaml = "JOB-"+job_name.replace("[","").replace("]","").replace(" ","_")
            j_info['settings']['name'] = job_name
            resources = dict()
            jobs = dict()
            job = dict()
            job[job_name_yaml]=j_info["settings"]
            jobs['jobs'] = job
            resources['resources']= jobs
            ymal_payload = yaml.dump(resources)
            self.__dump_Job(job_name_yaml,ymal_payload)


    def __list_DLTs(self):
        pipelines = []
        for p in self.__dbr_workspace.pipelines.list_pipelines():
            pipeline = self.__dbr_workspace.pipelines.get(p.pipeline_id)
            pipelines.append(pipeline.as_dict())
        return pipelines
    
    def __list_Jobs(self):
        jobs = []
        for j in self.__dbr_workspace.jobs.list():
            j_info = j.as_dict()
            job_details =  self.__dbr_workspace.jobs.get(j_info["job_id"])
            jobs.append(job_details.as_dict())
        return jobs
    
    def __clean_Jobs(self):
        files = glob.glob(f"{self.__jobs_folder}\\*")
        self.__remove_files(files)
    
    def __clean_DLTs(self):
        files = glob.glob(f"{self.__dlts_folder}\\*")
        self.__remove_files(files)

    def __dump_DLT(self,file_name, content):
        file_path = f"{self.__dlts_folder}\\{file_name}.yml"
        self.__save_dump(file_path,content)
    
    def __dump_Job(self,file_name, content):
        file_path = f"{self.__jobs_folder}\\{file_name}.yml"
        self.__save_dump(file_path,content)

    def __remove_files(self,files):
        for f in files:
            os.remove(f)
    
    def __save_dump(self,file_path, content):
        with open(file_path, 'w+') as f:
            f.write(content)

    def clean_DLTs_demo(self):
        files = glob.glob(f"{self.__dlts_folder}\\*")
        for f in files:
            if f.find('DLT-DLT-Gold-DWHSales-UC.yml')>0:
                continue
            os.remove(f)
    
    def clean_Jobs_demo(self):
        files = glob.glob(f"{self.__jobs_folder}\\*")
        for f in files:
            if f.find('JOB-SQLDay2024-Gold-Demo-UC.yml')>0:
                continue
            os.remove(f)
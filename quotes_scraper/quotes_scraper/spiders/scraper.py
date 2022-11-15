import scrapy
import json
from google.cloud import bigquery
from datetime import datetime
client=bigquery.Client()
# Titulo =//h1/a/text().get()
# Citas = //span[@class="text" and @itemprop="text"]/text().getall()
# Top ten tag =response.xpath('//div[contains(@class,"tags-box")]//span[@class="tag-item"]/a/text()').getall()
#next page response.xpath('//ul[@class="pager"]//li[@class="next"]/a/@href').get()
class Scrapy (scrapy.Spider):
    name = 'scraper'
    start_urls= [
        'https://backoffice.totalcheck.cl/userservice/login?u=system&pw=tigre5playa'
    ]
    custom_settings = {
        'FEED_URI' : 'resp.json',
        'FEED_FORMAT': 'json',
        'CONCURRENT_REQUEST': 24,
        'MEMUSAGE_LIMIT_MB': 2048,
        'MEMUSAGE_NOTIFY_MAIL': ['arix86.solutions@gmail.com'],
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'REDCRIFY',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'DUPEFILTER_DEBUG': True
               
    }
    
    def parse(self, response):
        
        query = '''
                SELECT `ttlchk-cloud.vehiculos_nuevos_prod.tarea`.contentref,`ttlchk-cloud.vehiculos_nuevos_prod.tarea`.nombre
                FROM `ttlchk-cloud.vehiculos_nuevos_prod.tarea` 
                JOIN `ttlchk-cloud.vehiculos_nuevos_prod.servicio` ON `ttlchk-cloud.vehiculos_nuevos_prod.servicio`.unique_id=`ttlchk-cloud.vehiculos_nuevos_prod.tarea`.contentref
                WHERE `ttlchk-cloud.vehiculos_nuevos_prod.tarea`.fecha_ini is not null and `ttlchk-cloud.vehiculos_nuevos_prod.tarea`.fecha_fin is null and  `ttlchk-cloud.vehiculos_nuevos_prod.servicio`.es_caso_devuelto = FALSE and TIMESTAMP(`ttlchk-cloud.vehiculos_nuevos_prod.tarea`.ultima_actualizacion) < TIMESTAMP('''+datetime.now()+''') 
            '''
        query_job = client.query(query)
        data=[]
        [data.append(row['contentref']) for row in query_job if row['contentref'] not in data]
        task=[{'nombre':reg['nombre'],'ctref':reg['contentref']} for reg in query_job ]
        ticket= response.xpath('string(//body)').re(r"\w+\-\w+\-\w+\-\w+\-\w+")[0]
        ctref= data[0]
        link='https://backofficedigital.totalcheck.cl/api/workflowservice/workflow/inscripcion/nodeid/'+ ctref + '?alf_ticket='+ticket
        data.pop(0)
        yield scrapy.Request(link, method="GET", callback=self.parse_callback, cb_kwargs= {'ticket' : ticket,'tareas':task,'respuesta':[],'contentref':data,'unique_id':ctref})
        
                
    def parse_callback(self, response, **kwargs):
        datar=json.loads(response.text)
        datos=kwargs['contentref']
        unique_id=kwargs['unique_id']
        ticket=kwargs['ticket']
        respuesta=kwargs['respuesta']
        tareas=kwargs['tareas']
        task_ctref=[{'nombre':t['nombre']} for t in tareas if unique_id in t['ctref']]
        lista=datar['response']['workflow_details']
        tareas_service=[{'tarea':i['name'],'status':i['status']} for i in lista if i['status']=="active"]
        resp={'unique_id':unique_id,'tareas':task_ctref,'tareas_service':tareas_service}
        respuesta.append(resp)
        if len(datos) > 0:
            contentref=datos[0]
            datos.pop(0)    
            link='https://backofficedigital.totalcheck.cl/api/workflowservice/workflow/inscripcion/nodeid/'+ contentref + '?alf_ticket='+ticket     
            print("*" * 30)
            print(len(datos))
            print("*" * 30)
            yield scrapy.Request(link,method="GET", callback=self.parse_callback,cb_kwargs= {'ticket' : ticket,'tareas':tareas,'respuesta':respuesta,'contentref':datos,'unique_id':contentref})
        else:
            no_rel=es=0
            answ={}
            body=[]
            exit=pos=0
            for ct in resp:
                bd_task=ct['tareas']
                services_task=ct['tareas_service']
                estado=[]
                position=[]
                for bd in bd_task:
                    for tw in services_task:
                        if bd['nombre'] in tw['tarea']:
                            exit+=1 
                            position.append(pos)
                        pos=pos+1
                    if exit==0:
                        es+=1
                        estado.append(bd['nombre'])
                    exit=pos=0
                if len(estado)>0:
                    answ={'estado':'cerrado','db':'abierto','tareas':estado}    
                if  len(position)>0:
                    for idx in sorted(position, reverse = True):
                        del services_task[idx]
                if  len(services_task)>0:
                    answ['no_rel']=services_task
                    no_rel+=len(services_task)
                if  len(answ)>0:
                    answ['contentref']=ct['unique_id']
                    body.append(answ)
            yield   {
                     
                     'respuesta':body
                }    

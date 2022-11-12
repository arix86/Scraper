import scrapy
import json
from google.cloud import bigquery
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
        
        query = """
                SELECT `ttlchk-cloud.vehiculos_nuevos_prod.tarea`.contentref,`ttlchk-cloud.vehiculos_nuevos_prod.tarea`.fecha_ini,`ttlchk-cloud.vehiculos_nuevos_prod.tarea`.nombre
                FROM `ttlchk-cloud.vehiculos_nuevos_prod.tarea` 
                JOIN `ttlchk-cloud.vehiculos_nuevos_prod.servicio` ON `ttlchk-cloud.vehiculos_nuevos_prod.servicio`.unique_id=`ttlchk-cloud.vehiculos_nuevos_prod.tarea`.contentref
                WHERE `ttlchk-cloud.vehiculos_nuevos_prod.tarea`.fecha_ini is not null and `ttlchk-cloud.vehiculos_nuevos_prod.tarea`.fecha_fin is null and  `ttlchk-cloud.vehiculos_nuevos_prod.servicio`.es_caso_devuelto = FALSE 
            """
        query_job = client.query(query)
        data=[]
        [data.append(row['contentref']) for row in query_job if row['contentref'] not in data]
        task=[{'fecha_ini':reg['fecha_ini'],'nombre':reg['nombre'],'ctref':reg['contentref']} for reg in query_job ]
        ticket= response.xpath('string(//body)').re(r"\w+\-\w+\-\w+\-\w+\-\w+")[0]
        link='https://backofficedigital.totalcheck.cl/searchservice/tenant/system/type/taskmanager_inscripcion/from/0/size/100/sort/inserted_at:asc?alf_ticket='+ ticket
        ctref= data[0]
        data_raw='''
            {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "contentref": "'''+ctref+'''"
                            }
                        }
                    ],
                    "should": [
                        {
                            "term": {
                                "deleted": false
                            }
                        },
                        {
                            "bool": {
                                "must_not": {
                                    "exists": {
                                        "field": "deleted"
                                    }
                                }
                            }
                        }
                    ]
                }
                }
            '''
        data.pop(0)
        yield scrapy.Request(link, method="POST", callback=self.parse_callback,body=data_raw, cb_kwargs= {'ticket' : ticket,'tareas':task,'respuesta':[],'contentref':data,'unique_id':ctref})
        
                
    def parse_callback(self, response, **kwargs):
        datar=json.loads(response.text)
        datos=kwargs['contentref']
        unique_id=kwargs['unique_id']
        ticket=kwargs['ticket']
        respuesta=kwargs['respuesta']
        tareas=kwargs['tareas']
        task_ctref=[{'nombre':t['nombre'],'fecha_ini':t['fecha_ini']} for t in tareas if unique_id in t['ctref']]
        lista=datar['response']['search']['hits']['hits']
        tareas_service=[{'tarea':i['_source']['name'],'status':i['_source']['status']} for i in lista if i['_source']['status']=="active"]
        resp={'unique_id':unique_id,'tareas':task_ctref,'tareas_service':tareas_service}
        respuesta.append(resp)
        if len(datos) > 0:
            contentref=datos[0]
            data_raw='''
                    {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "contentref": "'''+contentref+'''"
                                    }
                                }
                            ],
                            "should": [
                                {
                                    "term": {
                                        "deleted": false
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": {
                                            "exists": {
                                                "field": "deleted"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                        }
                    '''
            datos.pop(0)    
            link='https://backofficedigital.totalcheck.cl/searchservice/tenant/system/type/taskmanager_inscripcion/from/0/size/100/sort/inserted_at:asc?alf_ticket='+ ticket      
            print("*" * 30)
            print(len(datos))
            print("*" * 30)
            yield scrapy.Request(link,method="POST", callback=self.parse_callback,body=data_raw, cb_kwargs= {'ticket' : ticket,'tareas':tareas,'respuesta':respuesta,'contentref':datos,'unique_id':contentref})
        else:
                                  
            yield   {
                     
                     'respuesta':respuesta
                }    

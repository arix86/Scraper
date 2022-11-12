def parse_only_tags(self, response, **kwargs):
        if kwargs:
            tags=kwargs['tags']
        tags.extend(response.xpath('//span[@class="text" and @itemprop="text"]/text()').getall())    
        next_page_button_link=response.xpath('//ul[@class="pager"]//li[@class="next"]/a/@href').get()
        if(next_page_button_link):
            yield response.follow(next_page_button_link, callback=self.parse_only_tags, cb_kwargs={'tags': tags})
        else:
             yield {
                 'tags': tags
             }
    
    def parse(self, response):
        title= response.xpath('//h1/a/text()').get()
        tags = response.xpath('//span[@class="text" and @itemprop="text"]/text()').getall()
        top_tags= response.xpath('//div[contains(@class,"tags-box")]//span[@class="tag-item"]/a/text()').getall()
        top=getattr(self,'top', None)
        if top:
            top = int(top)
            top_tags=top_tags[:top]
            
        yield {
            'title' : title,
            'top'   : top_tags
            }
        next_page_button_link=response.xpath('//ul[@class="pager"]//li[@class="next"]/a/@href').get()
        if(next_page_button_link):
            yield response.follow(next_page_button_link, callback=self.parse_only_tags, cb_kwargs={'tags': tags})


        lista=datar['response']['search']['hits']['hits']
        tareas_service=[{'tarea':i['_source']['name'],'status':i['_source']['status']} for i in lista if i['_source']['status']=="active"]
            for i in lista:
                if i['_source']['status']=="active":
                tareas_service.append({'tarea':i['_source']['name'],'status':i['_source']['status'] }) 
                es=0
            no_rel=0
            answ={}
            body=[]
            for ct in respuesta:
                bd_task=ct['tareas']
                services_task=ct['tareas_service']
                estado=[]
                position=[]
                for bd in bd_task:
                    [estado.append(bd['nombre']) for tw in services_task if bd['nombre'] not in tw['tarea']]
                    [position.append(i) for i, st in enumerate(services_task) if bd['nombre'] in st['tarea']]
                if len(estado)>0:
                    answ={'estado':'cerrado','db':'abierto','tareas':estado}    
                    es+=len(estado)
                if  len(position)>0:
                    for idx in sorted(position, reverse = True):
                        del services_task[idx]
                if  len(services_task)>0:
                    answ['no_rel']=services_task
                    no_rel+=len(services_task)
                if  len(answ)>0:
                    answ['contentref']=ct['unique_id']
                    body.append(answ)       
# Prova 2 - módulo 9

Este é o repositório da segunda prova do módulo 9. Foi criado um cluster em kafka com um producer e um consumer, que gerenciam dados de sensores de qualidade de ar. O producer acessa um script de config.json para utilizar o formato de mensagem desejado e então simula leituras com a geração de dados aleatórios. Então, ele publica essas mensagens no tópico determinado no config.json. Já o consumer acessa essas mensagens, mostra elas no console segundo a formatação desejada do config.json e então salva as informações em um banco MongoDB.

Esse sistema é iniciado por um docker compose, que sobe os containers de kafka, zookeeper e mongo. A partir daí, é necessário rodar os scripts de producer.py e consumer.py, oferencendo também um config.json no formato esperado. 

```json
{
    "topic": "qualidadeAr",
    "console_format": "QUALIDADE DO AR AGORA: {nivel} de {tipoPoluente}, em {timestamp}, no sensor {idSensor}",
    "fields": {
        "idSensor": {
            "type": "string",
            "random": {
                "type": "array",
                "values": ["001", "002", "003"]
            }
        },
        "nivel": {
            "type": "integer",
            "random": {
                "type": "integer",
                "min": 0,
                "max": 100
            }
        },
        "timestamp": {
            "type": "date",
            "random": {
                "type": "timestamp"
            }
        },
        "tipoPoluente": {
            "type": "string",
            "random": {
                "type": "array",
                "values": ["PM2.5", "PM10", "CO", "NO2", "O3", "SO2"]
            }
        }


        
    }
}
```

É essencial que o nome de cada campo seja escrito da mesma forma na string de console_format, para que a string de template funcione corretamente no consumer.

Ademais, foram feitos testes para validar a persistência de dados e a integridade deles no ecossistema Kafka. Eles estão disponíveis na pasta tests e, em resumo, utilizam as funções de criação de producers e consumers, assim como uma conexão com o banco Mongo, para publicar uma mensagem de teste e averiguar sua chegada correta tanto no tópico do Kafka quanto no banco de dados.
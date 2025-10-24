from datetime import datetime, timezone
from flask import Flask, Response, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import json
import paho.mqtt.client as mqtt


# ********************* APLICAÇÃO FLASK ***************************
app = Flask("registro")

# Configurações do Banco
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://grupo3:Senai%40134@datascience-gp3.mysql.database.azure.com/db_medidor2'


server_name= 'datascience-gp3.mysql.database.azure.com'
port='3306'
username='grupo3'
password= 'Senai%40134'
database='db_medidor2'

certificado='DigiCertGlobalRootG2.crt.pem'

uri =f"mysql+pymysql://{username}:{password}@{server_name}:{port}/{database}"
ssl_certificado = f"?ssl_ca={certificado}"



app.config['SQLALCHEMY_DATABASE_URI'] = uri + ssl_certificado





# Instância do SQLAlchemy
mybd = SQLAlchemy(app)


# ********************* MODELO BANCO ******************************
class Registro(mybd.Model):
    __tablename__ = 'tb_registro'
    id = mybd.Column(mybd.Integer, primary_key=True, autoincrement=True)
    temperatura = mybd.Column(mybd.Numeric(10, 2))
    pressao = mybd.Column(mybd.Numeric(10, 2))
    altitude = mybd.Column(mybd.Numeric(10, 2))
    umidade = mybd.Column(mybd.Numeric(10, 2))
    co2 = mybd.Column(mybd.Numeric(10, 2))
    poeira = mybd.Column(mybd.Numeric(10, 2))
    tempo_registro = mybd.Column(mybd.DateTime)

    def to_json(self):
        return {
            "id": self.id,
            "temperatura": float(self.temperatura) if self.temperatura else None,
            "pressao": float(self.pressao) if self.pressao else None,
            "altitude": float(self.altitude) if self.altitude else None,
            "umidade": float(self.umidade) if self.umidade else None,
            "co2": float(self.co2) if self.co2 else None,
            "poeira": float(self.poeira) if self.poeira else None,
            "tempo_registro": self.tempo_registro.strftime('%Y-%m-%d %H:%M:%S')
            if self.tempo_registro else None
        }


# ********************* MQTT ****************************************
mqtt_dados = {}

import paho.mqtt.client as mqtt





def conexao_sensor(client, userdata, flags, rc):
    print("Conectado ao broker MQTT com código:", rc)
    client.subscribe("projeto_integrado/SENAI134/Cienciadedados/grupo3")

def msg_sensor(client, userdata, msg):
    global mqtt_dados

    valor = msg.payload.decode('utf-8')
    mqtt_dados = json.loads(valor)
    print(f"Mensagem Recebida: {mqtt_dados}")

    with app.app_context():
        try:
            temperatura = mqtt_dados.get('temperature')
            pressao = mqtt_dados.get('pressure')
            altitude = mqtt_dados.get('altitude')
            umidade = mqtt_dados.get('humidity')
            co2 = mqtt_dados.get('CO2')             # corrigido
            poeira = mqtt_dados.get('particula1')   # opcional: somar com particula2
            tempo_registro = mqtt_dados.get('timestamp')

            if tempo_registro is None:
                print("TimeStamp não encontrado no payload")
                return

            tempo_oficial = datetime.fromtimestamp(int(tempo_registro), tz=timezone.utc)

            novos_dados = Registro(
                temperatura=temperatura,
                pressao=pressao,
                altitude=altitude,
                umidade=umidade,
                co2=co2,
                poeira=poeira,
                tempo_registro=tempo_oficial
            )

            mybd.session.add(novos_dados)
            mybd.session.commit()
            print("✅ Dados inseridos no banco de dados com sucesso!")

        except Exception as e:
            print(f"❌ Erro ao processar dados do MQTT: {e}")
            mybd.session.rollback()

mqtt_client = mqtt.Client()
mqtt_client.on_connect = conexao_sensor
mqtt_client.on_message = msg_sensor
mqtt_client.connect("test.mosquitto.org", 1883, 60)

def start_mqtt():
    mqtt_client.loop_start()



# ********************* ENDPOINTS **********************************
@app.route("/registro", methods=["GET"])
def seleciona_registro():
    registros = Registro.query.all()
    registros_json = [registro.to_json() for registro in registros]
    return gera_resposta(200, "registro", registros_json)


@app.route("/registro/<id>", methods=["GET"])
def seleciona_registro_id(id):
    registro = Registro.query.filter_by(id=id).first()
    if registro:
        return gera_resposta(200, "registro", registro.to_json())
    else:
        return gera_resposta(404, "registro", {}, "REGISTRO NÃO ENCONTRADO")


@app.route("/registro/<id>", methods=["DELETE"])
def deletar_registro(id):
    registro = Registro.query.filter_by(id=id).first()
    if registro:
        try:
            mybd.session.delete(registro)
            mybd.session.commit()
            return gera_resposta(200, "registro", registro.to_json(), "DELETADO COM SUCESSO!")
        except Exception as e:
            print("Erro:", e)
            mybd.session.rollback()
            return gera_resposta(400, "registro", {}, "ERRO AO DELETAR!")
    else:
        return gera_resposta(404, "registro", {}, "REGISTRO NÃO ENCONTRADO")


@app.route("/dados", methods=["GET"])
def busca_dados():
    return jsonify(mqtt_dados)


@app.route("/dados", methods=["POST"])
def criar_dados():
    try:
        dados = request.get_json()
        if not dados:
            return jsonify({"erro": "Nenhum dado fornecido"}), 400

        print("Dados Recebidos:", dados)

        temperatura = dados.get('temperatura')
        pressao = dados.get('pressao')
        altitude = dados.get('altitude')
        umidade = dados.get('umidade')
        co2 = dados.get('co2')
        poeira = dados.get('poeira')
        timestamp_unix = dados.get('tempo_registro')

        try:
            tempo_oficial = datetime.fromtimestamp(int(timestamp_unix), tz=timezone.utc)
        except Exception as e:
            print("Erro ao converter timestamp:", e)
            return jsonify({"erro": "Timestamp inválido"}), 400

        novo_registro = Registro(
            temperatura=temperatura,
            pressao=pressao,
            altitude=altitude,
            umidade=umidade,
            co2=co2,
            poeira=poeira,
            tempo_registro=tempo_oficial
        )

        mybd.session.add(novo_registro)
        mybd.session.commit()
        print("Dados inseridos no banco via POST com sucesso!")

        return jsonify({"mensagem": "Dados Recebidos com Sucesso"}), 201

    except Exception as e:
        print("Erro ao processar POST:", e)
        mybd.session.rollback()
        return jsonify({"erro": "Falha ao processar os dados"}), 500


# ********************* FUNÇÃO AUXILIAR *****************************
def gera_resposta(status, nome_do_conteudo, conteudo, mensagem=False):
    body = {nome_do_conteudo: conteudo}
    if mensagem:
        body["mensagem"] = mensagem
    return Response(json.dumps(body), status=status, mimetype="application/json")


# ********************* MAIN ***************************************
# if __name__ == "__main__":
#     with app.app_context():
#         mybd.create_all()
start_mqtt()
app.run(port=5003, host="0.0.0.0", debug=True)

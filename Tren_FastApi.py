from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware 
from pydantic import BaseModel
import joblib
import pandas as pd

app = FastAPI(title="Predictiones API")

# <--- Para dar los permisos de acceso
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)



# Cargar el modelo 
model = joblib.load('Tren_modelo_champion.pkl')

# estructura de entrada con Pydantic
class ClientData(BaseModel):
    LLAMADAS_A_TIGO:float
    LLAMADAS_A_ORANGE:float
    NUM_RECARGAS_DEL_CLIENTE_90DIAS:float
    NUM_INGRESOS_DEL_CLIENTE:float
    NUM_RECARGAS_DEL_CLIENTE:float
    LLAMADA_INTER_EXPRESO:float


# Recibe datos de 1 cliente
@app.post("/predict")
def predict(data: ClientData):
    # Convertir el objeto Pydantic a DataFrame
    input_df = pd.DataFrame([data.dict()])
    
    # Predicción
    prediction = model.predict(input_df)[0]
    probability = model.predict_proba(input_df)[0][1]
    
    return {
        "renuncia": int(prediction),
        "probabilidad": round(float(probability), 4),
        "mensaje": "El cliente probablemente renunciará" if prediction == 1 else "El cliente se quedará"
    }
    


#Recibe datos de una lista de clientes con proceso batch
@app.post("/predict_batch")
def predict_batch(data: List[ClientData]):
    # Convertimos la lista de objetos a DataFrame de una sola vez
    input_df = pd.DataFrame([d.dict() for d in data])
    
    # Realizamos todas las predicciones al mismo tiempo (mucho más rápido)
    predictions = model.predict(input_df)
    probabilities = model.predict_proba(input_df)[:, 1]
    
    results = []
    for i in range(len(predictions)):
        results.append({
            "prediccion": int(predictions[i]),
            "probabilidad": round(float(probabilities[i]), 4)
        })
    
    return results
    
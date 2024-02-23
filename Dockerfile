FROM python:latest

WORKDIR /app

# Instala virtualenv
RUN pip install virtualenv

# Crea un entorno virtual
RUN virtualenv venv

# Activa el entorno virtual
RUN /bin/bash -c "source venv/bin/activate"

# Copia el código de la aplicación
COPY . .

# Instala las dependencias desde requirements.txt
RUN pip install -r requirements.txt

# Comando por defecto para ejecutar la aplicación
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

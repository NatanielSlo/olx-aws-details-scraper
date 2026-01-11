FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# 1. Dodajemy boto3 do instalacji (bez tego SQS nie zadziała)
RUN pip install awslambdaric playwright boto3

WORKDIR /var/task

# 2. Kopiujemy WSZYSTKO (cały folder projektu), a nie tylko podfoldery
# Dzięki temu struktura src/ i app/ zostanie zachowana
COPY . .

# Instalujemy Chromium i zależności systemowe
RUN python3 -m playwright install chromium
RUN python3 -m playwright install-deps chromium

# 3. KLUCZOWE: Dodajemy folder /var/task do ścieżki Pythona
# Bez tego Lambda powie "ModuleNotFoundError: No module named 'src'"
ENV PYTHONPATH="${PYTHONPATH}:/var/task"

# Komenda startowa
ENTRYPOINT [ "/usr/bin/python3", "-m", "awslambdaric" ]

# Ścieżka do handlera (plik app/app.py i w nim funkcja handler)
CMD [ "app.app.handler" ]
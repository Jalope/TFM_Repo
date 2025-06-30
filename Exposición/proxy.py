# proxy.py
from flask import Flask, request, Response, abort, redirect
import requests

app = Flask(__name__)

# 1) Tu dashboard público (igual que antes)
ALLOWED_PREFIX = "/public/dashboard/f271ef00-f4b6-4b0d-84fc-fe51af0a9fa5"
METABASE_BASE = "http://localhost:3000"

@app.before_request
def check_path():
    p = request.path
    # Permitimos sólo:
    allowed = (
        # ➔ Dashboard público y sus sub-rutas
        p == ALLOWED_PREFIX or p.startswith(ALLOWED_PREFIX + "/")
        # ➔ Assets “raíz”
        or p.startswith("/app/")       # por si se piden /app/...
        or p.startswith("/dist/")      # por si se piden /dist/...
        or p.startswith("/assets/")    # por si se piden /assets/...
        or p.startswith("/api/")       # llamadas AJAX de Metabase
        or p == "/favicon.ico"         # favicon
    )
    if not allowed:
        abort(403)

# Para que “/” redirija a tu dashboard
@app.route("/")
def home():
    return redirect(ALLOWED_PREFIX, code=302)

# Rutas de proxy (cogen todo lo permitido arriba)
@app.route(f"{ALLOWED_PREFIX}",             methods=["GET","POST","PUT","DELETE"])
@app.route(f"{ALLOWED_PREFIX}/<path:rest>", methods=["GET","POST","PUT","DELETE"])
@app.route("/app/<path:rest>",              methods=["GET"])
@app.route("/dist/<path:rest>",             methods=["GET"])
@app.route("/assets/<path:rest>",           methods=["GET"])
@app.route("/api/<path:rest>",              methods=["GET","POST","PUT","DELETE"])
@app.route("/favicon.ico",                   methods=["GET"])
def proxy(rest=""):
    orig_path = request.path

    # 2) Reescribir interna la URL destino si viene bajo ALLOWED_PREFIX
    if orig_path.startswith(ALLOWED_PREFIX + "/dist/"):
        dest_path = orig_path[len(ALLOWED_PREFIX):]       # “/dist/…”
    elif orig_path.startswith(ALLOWED_PREFIX + "/app/"):
        dest_path = orig_path[len(ALLOWED_PREFIX):]       # “/app/…”
    elif orig_path.startswith(ALLOWED_PREFIX + "/assets/"):
        dest_path = orig_path[len(ALLOWED_PREFIX):]       # “/assets/…”
    elif orig_path.startswith(ALLOWED_PREFIX + "/api/"):
        dest_path = orig_path[len(ALLOWED_PREFIX):]       # “/api/…”
    else:
        dest_path = orig_path

    dest_url = METABASE_BASE + dest_path

    # 3) Forward multipropósito
    resp = requests.request(
        method=request.method,
        url=dest_url,
        headers={k: v for k, v in request.headers if k.lower() != "host"},
        params=request.args,
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
    )

    excluded = ["content-encoding", "content-length", "transfer-encoding", "connection"]
    headers = [
        (name, value)
        for name, value in resp.raw.headers.items()
        if name.lower() not in excluded
    ]
    return Response(resp.content, resp.status_code, headers)

if __name__ == "__main__":
    app.run(port=8080)
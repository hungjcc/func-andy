import azure.functions as func
import logging

a = 2
b = 3
c = a + b

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
#decorator
@app.route(route="func_andy")



def func_andy(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             f"This is a trigger by GitHub Actions. HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response. The sum of a + b = {c}",
             status_code=200
        )

from flask import Flask, request, jsonify, render_template_string

from rpyc_util import RpycUtil


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

rpyc = RpycUtil('localhost', 54345)

@app.route('/')
def index():
    params = dict(
        args=[],
        replace_existing=True,
        id='get_weather_info'
    )
    result = rpyc.add_job_json(params)
    print(repr(result))
    return "Hello World"

if __name__=='__main__':
    run_cfg = dict(
        debug=True,
        host='0.0.0.0',
        port=5665
    )
    app.run(**run_cfg)

from flask import Flask, jsonify
import subprocess

app = Flask(__name__)

KNIME_EXE    = r"C:\Users\Samee\AppData\Local\Programs\KNIME\knime.exe"
KNIME_WF_DIR = r"C:\Users\Samee\knime-workspace\job_market_cleaning_workflow_real1_import"

@app.route("/run-knime", methods=["POST"])
def run_knime():
    result = subprocess.run([
        KNIME_EXE, "-nosave", "-consoleLog", "-nosplash", "-reset",
        "-application", "org.knime.product.KNIME_BATCH_APPLICATION",
        f"-workflowDir={KNIME_WF_DIR}"
    ], capture_output=True, text=True, timeout=3600)

    if result.returncode != 0:
        return jsonify({"status": "error", "message": result.stderr[-500:]}), 500

    return jsonify({"status": "success"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8005)
# BuildBrain — Server Setup & Restart Guide

## Overview

BuildBrain runs on a Windows Server 2019 VPS at IP `45.146.160.105`.
It uses two components:
- **Flask app** — the Python web server (port 5000)
- **ngrok** — the tunnel that makes it publicly accessible at https://buildbrain.ngrok.app

Both must be running at the same time for the site to work.

---

## How to Restart After a Server Reboot

RDP into the VPS, then open **two separate Command Prompt windows**.

**Window 1 — Start the web app:**
```
cd C:\buildbrain
python app.py
```
You should see:
```
BuildBrain Web App
Running on http://0.0.0.0:5000
```

**Window 2 — Start the tunnel:**
```
C:\ngrok\ngrok.exe http --url=buildbrain.ngrok.app 5000
```
You should see:
```
Forwarding: https://buildbrain.ngrok.app -> http://localhost:5000
```

Check https://buildbrain.ngrok.app in a browser — it should load immediately.

**Do not close either window.** If you close one, the site goes down.

---

## Full Fresh Install (if server is wiped)

Follow these steps in order. Open Command Prompt as Administrator.

---

### Step 1 — Install Python

```
curl -o %TEMP%\python-installer.exe https://www.python.org/ftp/python/3.12.9/python-3.12.9-amd64.exe
```
```
%TEMP%\python-installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_pip=1
```

Wait 60 seconds. Close and reopen Command Prompt. Verify:
```
python --version
```

---

### Step 2 — Install Git

```
curl -o %TEMP%\git-installer.exe -L https://github.com/git-for-windows/git/releases/download/v2.47.1.windows.2/Git-2.47.1.2-64-bit.exe
```
```
%TEMP%\git-installer.exe /VERYSILENT /NORESTART
```

Wait 60 seconds. Close and reopen Command Prompt. Verify:
```
git --version
```

---

### Step 3 — Clone the repo and install dependencies

```
git clone https://github.com/chazferr/buildbrainv1.git C:\buildbrain
```
```
cd C:\buildbrain
```
```
pip install -r requirements.txt
```

---

### Step 4 — Create the API key file

```
echo ANTHROPIC_API_KEY=YOUR_API_KEY_HERE> C:\buildbrain\.env
```
Get the current API key from Chaz or from https://console.anthropic.com/settings/keys

Verify it saved correctly:
```
type C:\buildbrain\.env
```
Should show: `ANTHROPIC_API_KEY=sk-ant-api03-AjA73...`

---

### Step 5 — Install ngrok

```
curl -o %TEMP%\ngrok.zip -L https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-windows-amd64.zip
```
```
powershell -command "Expand-Archive -Path '%TEMP%\ngrok.zip' -DestinationPath 'C:\ngrok' -Force"
```
```
C:\ngrok\ngrok.exe config add-authtoken 39Sn0FxDGeQZJ6gM05ndFODwdUT_6aUFWVqXkka7nMurkmvvP
```

---

### Step 6 — Create the startup batch file

Run these four commands one at a time:
```
echo @echo off > C:\buildbrain\start.bat
```
```
echo start /B python C:\buildbrain\app.py >> C:\buildbrain\start.bat
```
```
echo start /B C:\ngrok\ngrok.exe http --url=buildbrain.ngrok.app 5000 >> C:\buildbrain\start.bat
```
```
schtasks /create /tn "BuildBrain" /tr "C:\buildbrain\start.bat" /sc onstart /ru SYSTEM /rl highest /f
```

---

### Step 7 — Start it now

Open two Command Prompt windows and run:

**Window 1:**
```
cd C:\buildbrain && python app.py
```

**Window 2:**
```
C:\ngrok\ngrok.exe http --url=buildbrain.ngrok.app 5000
```

Visit https://buildbrain.ngrok.app — BuildBrain is live.

---

## Key Information

| Item | Value |
|---|---|
| Site URL | https://buildbrain.ngrok.app |
| VPS IP | 45.146.160.105 |
| App directory | C:\buildbrain |
| ngrok directory | C:\ngrok |
| ngrok account | chaz (Pay-as-you-go) |
| GitHub repo | https://github.com/chazferr/buildbrainv1 |

---

## Updating the Code

When new code is pushed to GitHub, pull it on the server:

1. Stop the Flask app (Ctrl+C in Window 1)
2. Run:
```
cd C:\buildbrain
git pull
```
3. Restart: `python app.py`

ngrok does not need to be restarted when updating code.

---

## Troubleshooting

**Site is down / ERR_NGROK_8012**
- Flask app is not running. Start it in Window 1: `cd C:\buildbrain && python app.py`

**401 errors / extraction failing**
- API key may be expired. Get a new key from https://console.anthropic.com/settings/keys
- Update C:\buildbrain\.env with the new key and restart the Flask app

**ngrok says domain already in use**
- Another ngrok instance is running. Run: `taskkill /F /IM ngrok.exe` then restart ngrok

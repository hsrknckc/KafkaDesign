const { app, BrowserWindow, ipcMain } = require("electron");
const { spawn } = require("child_process");

const db = require("./db.js");
const path = require("path");
const kafkaPath = app.isPackaged
  ? path.join(process.execPath, "../../../k/")
  : path.join(__dirname, "../k/");
const kafkaDir = kafkaPath;
const powerShell = "powershell.exe";

const user = {};
let portNumber = 9092;

/**
 * Creates the main application window.
 */
function createWindow() {
  const win = new BrowserWindow({
    width: 800,
    height: 600,
    webPreferences: {
      // It's a best practice to use a preload script to expose
      // Node.js APIs to the renderer process securely.
      nodeIntegration: true,
      contextIsolation: false,
    },
    autoHideMenuBar: true,
  });

  //test

  // Load the index.html of the app.
  win.loadFile("index.html");

  win.on("close", () => {
    console.log("pencere kapanıyor");
    db.closeTime(new Date().toUTCString(), user.username);
    console.log("çıkış zamanı kaydedildi");
  });
}

// This method will be called when Electron has finished
// initialization and is ready to create browser windows.
app.whenReady().then(createWindow);

// Activate event listener for macOS.
app.on("activate", () => {
  // On macOS it's common to re-create a window in the app when the
  // dock icon is clicked and there are no other windows open.
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

// Quit when all windows are closed, except on macOS. There, it's common
// for applications and their menu bar to stay active until the user quits
// explicitly with Cmd + Q.
app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

zookeeperCatched = false;

// Kullanıcı giriş işlemi
ipcMain.on("login", (event, { username, password }) => {
  db.authenticate(username, password, (err, result) => {
    if (err) {
      event.sender.send("login-result", { success: false, error: err.message });
      return;
    }
    if (!result) {
      event.sender.send("login-result", { success: false });
      return;
    }
    event.sender.send("login-result", { success: true, role: result.role });
    user.username = username;
  });
});

ipcMain.on("run-zookeeper", (event) => {
  const runZookeeperCommand =
    "./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties";
  const ps = spawn(powerShell, ["-Command", runZookeeperCommand], {
    cwd: kafkaDir,
  });

  const expectedOutput = "INFO Created server with tickTime";

  ps.stdout.on("data", (data) => {
    const output = data.toString();
    console.log(`Ps çıktısı : ${output}`);

    if (output.includes(expectedOutput) && !zookeeperCatched) {
      event.sender.send("zookeeper-output", { status: "running", output });
      zookeeperCatched = true;
    } else if (!zookeeperCatched) {
      event.sender.send("zookeeper-output", { status: "checking", output });
    }
  });

  ps.stderr.on("data", (data) => {
    console.error(`ps hatası: ${data}`);
    event.sender.send("zookeeper-output", {
      status: "error",
      output: data.toString(),
    });
  });

  ps.on("close", (code) => {
    console.log(`child proc exit ${code}`);
    event.sender.send("zookeeper-output", {
      status: "completed",
      output: ` `,
    });
    zookeeperCatched = false;
  });
});

ipcMain.on("run-kafka", (event, port) => {
  portNumber = port || 9092;
  const runKafkaCommand = `$env:JMX_PORT=9999 ; ./bin/windows/kafka-server-start.bat ./config/server-0.properties --override listeners=SASL_SSL://localhost:${portNumber} --override advertised.listeners=SASL_SSL://localhost:${portNumber}`;

  const ps = spawn(powerShell, ["-Command", runKafkaCommand], {
    cwd: kafkaDir,
  });

  const expectedOutput =
    "[KafkaServer id=0] started (kafka.server.KafkaServer)";
  cathced = false;

  ps.stdout.on("data", (data) => {
    const output = data.toString();
    console.log(`Ps çıktısı : ${output}`);

    if (output.includes(expectedOutput) && !cathced) {
      event.sender.send("kafka-output", { status: "running", output });
      cathced = true;
    } else if (!cathced) {
      event.sender.send("kafka-output", { status: "checking", output });
    }
  });

  ps.stderr.on("data", (data) => {
    console.error(`ps hatası: ${data}`);
    event.sender.send("kafka-output", {
      status: "error",
      output: data.toString(),
    });
  });

  ps.on("close", (code) => {
    console.log(`child proc exit ${code}`);
    event.sender.send("kafka-output", {
      status: "completed",
      output: ` `,
    });
  });
});

ipcMain.on("check-kafka", (event) => {
  const runningCheckCommand = "netstat -ano -p tcp | findstr /i 9092";
  const ps = spawn(powerShell, [runningCheckCommand]);
  ps.stdout.on("data", (data) => {
    const output = data.toString();
    if (output.includes("9092")) {
      console.log("KAFKA AKTIF");
      event.sender.send("kafka-output", { status: "running", output: "" });
    } else {
      console.log("else çalıştı");
    }
  });
});

ipcMain.on("check-zookeeper", (event) => {
  const runningCheckCommand = "netstat -ano -p tcp | findstr /i 2182";
  const ps = spawn(powerShell, [runningCheckCommand]);
  ps.stdout.on("data", (data) => {
    const output = data.toString();
    if (output.includes("2182")) {
      console.log("ZK AKTIF");
      event.sender.send("zookeeper-output", { status: "running", output: "" });
    } else {
      console.log("else çalıştı");
    }
  });
});

ipcMain.on("stop-kafka", (event) => {
  const kafkaStopCommand = "./bin/windows/kafka-server-stop.bat";
  const ps = spawn(powerShell, ["-Command", kafkaStopCommand], {
    cwd: kafkaDir,
  });

  event.sender.send("kafka-output", {
    output: "kafka kapatılıyor",
  });

  ps.stderr.on("data", (data) => {
    console.error(`ps hatası: ${data}`);
  });

  ps.on("close", (code) => {
    console.log(`child proc exit ${code}`);

    const zkDeleteCommand =
      "./bin/windows/zookeeper-shell.bat localhost:2182 -zk-tls-config-file ./config/zookeeper-client.properties delete /brokers/ids/0";

    const zk = spawn(powerShell, ["-Command", zkDeleteCommand], {
      cwd: kafkaDir,
    });

    zk.stdout.on("data", (data) => {
      event.sender.send("kafka-output", {
        output: `kafkanın zookeeper node'u siliniyor`,
      });
    });
    zk.stderr.on("data", (data) => {
      console.error(`Zookeeper temizleme hatası: ${data.toString()}`);
    });
    zk.on("close", () => {
      console.log("kafka zk node silindi ");
      event.sender.send("kafka-output", {
        status: "completed",
        output: "kafka broker tamamen kapatıldı",
      });
    });
  });
});

ipcMain.on("stop-zookeeper", (event) => {
  const zkDeleteCommand =
    "./bin/windows/zookeeper-shell.bat localhost:2182 -zk-tls-config-file ./config/zookeeper-client.properties delete /brokers/ids/0";

  const zk = spawn(powerShell, ["-Command", zkDeleteCommand], {
    cwd: kafkaDir,
  });
  zk.stdout.on("data", (data) => {
    event.sender.send("zookeeper-output", {
      output: `kafkanın zookeeper node'u siliniyor`,
    });
  });
  zk.stderr.on("data", (data) => {
    console.error(`Zookeeper temizleme hatası: ${data.toString()}`);
  });
  zk.on("close", () => {
    console.log("kafka zk node silindi ");
    const zookeeperStopCommand = "./bin/windows/zookeeper-server-stop.bat";
    const ps = spawn(powerShell, ["-Command", zookeeperStopCommand], {
      cwd: kafkaDir,
    });

    ps.stderr.on("data", (data) => {
      console.error(`ps hatası: ${data}`);
    });

    ps.on("close", (code) => {
      console.log(`child proc exit ${code}`);
      event.sender.send("zookeeper-output", {
        status: "completed",
        output: "",
      });
    });
    event.sender.send("zookeeper-output", {
      status: "completed",
      output: "zookeeper tamamen kapatıldı",
    });
  });
});

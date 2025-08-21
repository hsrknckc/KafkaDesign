const {ipcRenderer} = require("electron");

// Giriş işlemleri
const loginForm = document.getElementById("loginForm");
const loginStatus = document.getElementById("loginStatus");
const mainContent = document.getElementById("mainContent");
const adminPanel = document.getElementById("adminPanel");
const loginContainer = document.getElementById("loginContainer");
const loginTitle = document.getElementById("loginTitle");

let currentUserRole = null;

if (loginForm) {
    loginForm.addEventListener("submit", function(e) {
        e.preventDefault();
        const username = document.getElementById("username").value;
        const password = document.getElementById("password").value;
        ipcRenderer.send("login", { username, password });
    });
}

ipcRenderer.on("login-result", (event, result) => {
    if (result.success) {
        loginStatus.textContent = "Giriş başarılı!";
        mainContent.style.display = "block";
    // Tüm login alanlarını gizle
    if (loginContainer) loginContainer.style.display = "none";
    if (loginTitle) loginTitle.style.display = "none";
    if (loginForm) loginForm.style.display = "none";
        currentUserRole = result.role;
        if (result.role === "admin") {
            adminPanel.style.display = "block";
        } else {
            adminPanel.style.display = "none";
        }
    } else {
        loginStatus.textContent = "Giriş başarısız!";
    }
});


ipcRenderer.send("check-kafka");
ipcRenderer.send("check-zookeeper");


// Admin paneli: Kafka portunu değiştirme
const changeKafkaPortBtn = document.getElementById("changeKafkaPortBtn");
const kafkaPortInput = document.getElementById("kafkaPort");
const portChangeStatus = document.getElementById("portChangeStatus");

if (changeKafkaPortBtn) {
    changeKafkaPortBtn.addEventListener("click", function() {
        const newPort = kafkaPortInput.value;
        if (!newPort) {
            portChangeStatus.textContent = "Lütfen bir port girin.";
            return;
        }
        // Kafka'yı yeni port ile başlatacak komut oluştur
        const commandToRun = `cd D:/kodlar/aa/kafka_yeni/ ; ./bin/windows/kafka-server-start.bat ./config/server-0.properties --override listeners=SASL_SSL://localhost:${newPort} --override advertised.listeners=SASL_SSL://localhost:${newPort}`;
        const expectedText = `[KafkaServer id=0] started (kafka.server.KafkaServer)`;
        ipcRenderer.send("run-kafka", {command:commandToRun,expectedOutput:expectedText});
        portChangeStatus.textContent = `Kafka ${newPort} portu ile başlatıldı!`;
    });
}



const kafkaRunBtn = document.getElementById("kafkaRunBtn");
const kafkaStatusDiv = document.getElementById("kafkaStatus");
const kafkaOutputPre = document.getElementById("kafkaOutput");

kafkaRunBtn.addEventListener("click", ()=>{
    kafkaStatusDiv.textContent = "Durum: çalıştırılıyor...";
    kafkaOutputPre.textContent = " ";

    ipcRenderer.send("run-kafka");
});


ipcRenderer.on("kafka-output", (event, {status,output}) => {
    kafkaOutputPre.textContent = output + "\n";

    if(status === "running"){
        kafkaStatusDiv.textContent = "Durum: çalışıyor!!";
        kafkaStatusDiv.className="running";
        kafkaRunBtn.disabled=true;
        changeKafkaPortBtn.disabled=true;
    }else if(status === "checking"){
        kafkaStatusDiv.textContent = "Durum: başlatılıyor";
        kafkaStatusDiv.className="checking";
    }else if(status === "error"){
        kafkaStatusDiv.textContent = "Durum: hata!!";
        kafkaStatusDiv.className="error";
    }else if(status === "closing"){
        kafkaStatusDiv.textContent = "Durum: kapanıyor...";
        kafkaStatusDiv.className="closing"; 
    }else if(status==="completed"){
        kafkaStatusDiv.textContent = "Durum: kapandı!!";
        kafkaStatusDiv.className="";
        kafkaRunBtn.disabled=false;
        changeKafkaPortBtn.disabled=false;   
    }
})

const zookeeperRunBtn = document.getElementById("zookeeperRunBtn");
const zookeeperStatusDiv = document.getElementById("zookeeperStatus");
const zookeeperOutputPre = document.getElementById("zookeeperOutput");

zookeeperRunBtn.addEventListener("click", ()=>{
    zookeeperStatusDiv.textContent = "Durum: çalıştırılıyor...";
    zookeeperOutputPre.textContent = " ";   

    ipcRenderer.send("run-zookeeper");
});

ipcRenderer.on("zookeeper-output", (event, {status,output}) => {
    zookeeperOutputPre.textContent = output + "\n";

    if(status === "running"){
        zookeeperStatusDiv.textContent = "Durum: çalışıyor!!";
        zookeeperStatusDiv.className="running";
        zookeeperRunBtn.disabled=true;
        kafkaRunBtn.disabled=false;
        changeKafkaPortBtn.disabled=false;
    }else if(status === "checking"){
        zookeeperStatusDiv.textContent = "Durum: başlatılıyor";
        zookeeperStatusDiv.className="checking";
    }else if(status === "error"){
        zookeeperStatusDiv.textContent = "Durum: hata!!";
        zookeeperStatusDiv.className="error";
    }else if(status==="completed"){
        zookeeperStatusDiv.textContent = "Durum: kapandı!!";
        zookeeperStatusDiv.className="";
        zookeeperRunBtn.disabled=false;
        kafkaRunBtn.disabled=true;
        changeKafkaPortBtn.disabled=true;
    }
});

const zookeeperStopBtn = document.getElementById("zookeeperStopBtn");

zookeeperStopBtn.addEventListener("click", ()=>{
    ipcRenderer.send("stop-zookeeper");
});

const kafkaStopBtn = document.getElementById("kafkaStopBtn");

kafkaStopBtn.addEventListener("click", ()=>{
    ipcRenderer.send("stop-kafka");
});

const {ipcRenderer} = require("electron");

// Giriş işlemleri
const loginForm = document.getElementById("loginForm");
const loginStatus = document.getElementById("loginStatus");
const mainContent = document.getElementById("mainContent");
const adminPanel = document.getElementById("adminPanel");
const loginContainer = document.getElementById("loginContainer");
const loginTitle = document.getElementById("loginTitle");
const topicsList = document.getElementById("topicsList");
const usersList = document.getElementById("usersList");


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
            topicsList.style.display="none";
        }
    } else {
        loginStatus.textContent = "Giriş başarısız!";
    }
});


ipcRenderer.send("check-zookeeper");
ipcRenderer.send("check-kafka");


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
        ipcRenderer.send("run-kafka", {port:newPort});
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
    }else if(status === "checking"){
        kafkaStatusDiv.textContent = "Durum: başlatılıyor";
    }else if(status === "error"){
        kafkaStatusDiv.textContent = "Durum: hata!!";
    }else if(status === "closing"){
        kafkaStatusDiv.textContent = "Durum: kapanıyor...";
    }else if(status==="completed"){
        kafkaStatusDiv.textContent = "Durum: kapandı!!";
    }

    switch (kafkaStatusDiv.textContent){
        case "Durum: çalışıyor!!":
            kafkaRunBtn.disabled=true;
            changeKafkaPortBtn.disabled=true;
            break;
        case "Durum: kapandı!!":
            kafkaRunBtn.disabled=false;
            changeKafkaPortBtn.disabled=false;
            break;
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
    }else if(status === "checking"){
        zookeeperStatusDiv.textContent = "Durum: başlatılıyor";
    }else if(status === "error"){
        zookeeperStatusDiv.textContent = "Durum: hata!!";
    }else if(status==="completed"){
        zookeeperStatusDiv.textContent = "Durum: kapandı!!";
    }

    switch(zookeeperStatusDiv.textContent){
        case "Durum: çalışıyor!!":
            zookeeperRunBtn.disabled=true;
            kafkaRunBtn.disabled=false;
            changeKafkaPortBtn.disabled=false;
            break;
        case "Durum: kapandı!!":
            zookeeperRunBtn.disabled=false;
            kafkaRunBtn.disabled=true;
            changeKafkaPortBtn.disabled=true;
            break;
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



// ipcRenderer.on("list-topics", (event, { output }) => {
//     topicsList.textContent += output;
// });

// const refreshTopicsBtn = document.getElementById("refreshTopicsBtn");

// refreshTopicsBtn.addEventListener("click", ()=>{
//     topicsList.textContent = "Yükleniyor...";
//     ipcRenderer.send("list-topics");
// });

// const refreshUsersBtn = document.getElementById("refreshUsersBtn");

// refreshUsersBtn.addEventListener("click", ()=>{
//     usersList.textContent = "Yükleniyor...";
//     ipcRenderer.send("list-users");
// });

// ipcRenderer.on("list-users", (event, { output }) => {
//     usersList.textContent += output;
// });

const clockContainer = document.querySelector(".js-clock"),
clockTitle = clockContainer.querySelector(".clock-time");
clockSecond = clockContainer.querySelector(".clock-time-sec");

function getTime(){
    const date = new Date();
    const seconds = date.getSeconds();
    const minutes = date.getMinutes();
    const hours = date.getHours();

    clockTitle.innerText = `${hours < 10 ? `0${hours}` : hours}:${minutes < 10 ? `0${minutes}` : minutes}`;
    clockSecond.innerText = `  ${seconds < 10 ? `0${seconds}` : seconds}`;
    
}

function init(){
    getTime();
    setInterval(getTime, 1000);
}

init();
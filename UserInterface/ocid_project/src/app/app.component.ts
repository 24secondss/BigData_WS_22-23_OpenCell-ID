import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'OpenCellID_Project';
  longitude = "";
  latitude = "";
  result: string | any[] = [];
  GSM = "";
  UMTS = "";
  CDMA = "";
  LTE = "";
  spin = "false";
  color = "accent"
  
  async query(){
    this.spin = "true";
    this.GSM = "";
    this.UMTS = "";
    this.CDMA = "";
    this.LTE = "";
    const radioSignal = ["GSM","UMTS","CDMA","LTE"]; 
    for(let i in radioSignal){
      this.result = this.result.concat(
        await fetch("http://"+ self.location.host + "/" + this.longitude + "/" + this.latitude + "/" + radioSignal[i])
        .then(response => response.json())
        .then(queryResult =>  {
          if (queryResult.queryResult.length > 0){
            console.log("QueryResutl Frontend");
            console.log(queryResult.queryResult);
            return queryResult.queryResult;
          }
          else {
            console.log("QueryResutl Frontend");
            console.log(queryResult.queryResult);
            return { avgRange: "None" }
          }
        }));
      if (radioSignal[i] == "GSM") {
        if(this.result[0].avgRange <= 1000){
          this.GSM = "poor";
        }
        else if (this.result[0].avgRange <= 1500){
          this.GSM = "medium";
        }
        else if(this.result[0].avgRange = "None"){
          this.GSM = "None";
        }
        else {
          this.GSM = "great";
        }
      }
      if (radioSignal[i] == "UMTS") {
        if(this.result[1].avgRange <= 1000){
          this.UMTS = "poor";
        }
        else if (this.result[1].avgRange <= 1500){
          this.UMTS = "medium";
        }
        else if(this.result[1].avgRange = "None"){
          this.UMTS = "None";
        }
        else {
          this.UMTS = "great";
        }
      }
      if (radioSignal[i] == "CDMA") {
        if(this.result[2].avgRange <= 1000){
          this.CDMA = "poor";
        }
        else if (this.result[2].avgRange <= 1500){
          this.CDMA = "medium";
        }
        else if(this.result[2].avgRange = "None"){
          this.CDMA = "None";
        }
        else {
          this.CDMA = "great";
        }
      }
      if (radioSignal[i] == "LTE") {
        if(this.result[3].avgRange <= 1000){
          this.LTE = "poor";
        }
        else if (this.result[3].avgRange <= 1500){
          this.LTE = "medium";
        }
        else if(this.result[3].avgRange = "None"){
          this.LTE = "None";
        }
        else {
          this.LTE = "great";
        }
      }
    };
    this.spin = "false";
  }
}

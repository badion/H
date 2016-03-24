package com.cert.badion;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlightDataModel implements Writable,
		WritableComparable<FlightDataModel> {
	private static final long serialVersionUID = 1L;
	private Integer year;
	private Integer month;
	private Integer dayOfMonth;
	private Integer dayOfWeek;
	private Integer depTime;
	private Integer crsDepTime;
	private Integer arrTime;
	private String uniqueCarrier;
	private Integer flightNum;
	private String tailNum;
	private Integer actualElapsedTime;
	private Integer crsElapsedTime;
	private Integer airTime;
	private Integer arrDelay;
	private Integer depDelay;
	private String origin;
	private String dest;
	private String distance;
	private String TaxiIn;
	private String TaxiOut;
	private String ancelled;
	private String cancellationCode;
	private String diverted;
	private String carrierDelay;
	private String weatherDelay;
	private String nasDelay;
	private String securityDelay;
	private String lateAircraftDelay;
	private String minTemp;
	private String maxTemp;

	public FlightDataModel(String maxTemp) {
		super();
		this.maxTemp = maxTemp;
	}

	public String getMinTemp() {
		return minTemp;
	}

	public void setMinTemp(String minTemp) {
		this.minTemp = minTemp;
	}

	public String getMaxTemp() {
		return maxTemp;
	}

	public void setMaxTemp(String maxTemp) {
		this.maxTemp = maxTemp;
	}

	public FlightDataModel() {
		// TODO Auto-generated constructor stub
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(dayOfMonth);
		// out.writeInt(dayOfWeek);
		out.writeInt(depTime);
		// out.writeInt(crsDepTime);
		out.writeInt(arrTime);
		out.writeUTF(uniqueCarrier);
		out.writeInt(flightNum);
		// out.writeUTF(tailNum);
		out.writeInt(actualElapsedTime);
		// out.writeInt(crsElapsedTime);
		// out.writeInt(airTime);
		out.writeInt(arrDelay);
		out.writeInt(depDelay);
		out.writeUTF(origin);
		out.writeUTF(dest);
		// out.writeUTF(distance);
		// out.writeUTF(TaxiIn);
		// out.writeUTF(TaxiOut);
		// out.writeUTF(ancelled);
		// out.writeUTF(cancellationCode);
		// out.writeUTF(diverted);
		// out.writeUTF(carrierDelay);
		// out.writeUTF(weatherDelay);
		// out.writeUTF(nasDelay);
		// out.writeUTF(securityDelay);
		// out.writeUTF(lateAircraftDelay);
		out.writeUTF(minTemp);
		out.writeUTF(maxTemp);

	}

	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		dayOfMonth = in.readInt();
		// dayOfWeek = in.readInt();
		depTime = in.readInt();
		// crsDepTime = in.readInt();
		arrTime = in.readInt();
		uniqueCarrier = in.readUTF();
		flightNum = in.readInt();
		// tailNum = in.readUTF();
		actualElapsedTime = in.readInt();
		// crsElapsedTime = in.readInt();
		// airTime = in.readInt();
		arrDelay = in.readInt();
		depDelay = in.readInt();
		origin = in.readUTF();
		dest = in.readUTF();
		// distance = in.readUTF();
		// TaxiIn = in.readUTF();
		// TaxiOut = in.readUTF();
		// ancelled = in.readUTF();
		// cancellationCode = in.readUTF();
		// diverted = in.readUTF();
		// carrierDelay = in.readUTF();
		// weatherDelay = in.readUTF();
		// nasDelay = in.readUTF();
		// securityDelay = in.readUTF();
		// lateAircraftDelay = in.readUTF();
		minTemp = in.readUTF();
		maxTemp = in.readUTF();
	}

	@Override
	public String toString() {
		return year + "," + month + "," + dayOfMonth + "," + depTime + ","
				+ arrTime + "," + uniqueCarrier + "," + flightNum + ","
				+ actualElapsedTime + "," + arrDelay + "," + depDelay + ","
				+ origin + "," + dest + "," + minTemp + "," + maxTemp;
	}

	public int compareTo(FlightDataModel o) {
		Date date1 = new Date();
		Date date2 = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		try {
			date1 = formatter.parse(year + "-" + month
					+ "-" + dayOfMonth);
			date2 = formatter.parse(o.getYear() + "-" + o.getMonth()
					+ "-" + o.getDayOfMonth());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		int compYear = date1.compareTo(date2);
		return compYear;
	}

	public FlightDataModel(Integer year, Integer month, Integer dayOfMonth,
			Integer dayOfWeek, Integer depTime, Integer crsDepTime,
			Integer arrTime, String uniqueCarrier, Integer flightNum,
			String tailNum, Integer actualElapsedTime, Integer crsElapsedTime,
			Integer airTime, Integer arrDelay, Integer depDelay, String origin,
			String dest, String distance, String taxiIn, String taxiOut,
			String ancelled, String cancellationCode, String diverted,
			String carrierDelay, String weatherDelay, String nasDelay,
			String securityDelay, String lateAircraftDelay) {
		this.year = year;
		this.month = month;
		this.dayOfMonth = dayOfMonth;
		this.dayOfWeek = dayOfWeek;
		this.depTime = depTime;
		this.crsDepTime = crsDepTime;
		this.arrTime = arrTime;
		this.uniqueCarrier = uniqueCarrier;
		this.flightNum = flightNum;
		this.tailNum = tailNum;
		this.actualElapsedTime = actualElapsedTime;
		this.crsElapsedTime = crsElapsedTime;
		this.airTime = airTime;
		this.arrDelay = arrDelay;
		this.depDelay = depDelay;
		this.origin = origin;
		this.dest = dest;
		this.distance = distance;
		this.TaxiIn = taxiIn;
		this.TaxiOut = taxiOut;
		this.ancelled = ancelled;
		this.cancellationCode = cancellationCode;
		this.diverted = diverted;
		this.carrierDelay = carrierDelay;
		this.weatherDelay = weatherDelay;
		this.nasDelay = nasDelay;
		this.securityDelay = securityDelay;
		this.lateAircraftDelay = lateAircraftDelay;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Integer getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(Integer dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public Integer getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(Integer dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public Integer getDepTime() {
		return depTime;
	}

	public void setDepTime(Integer depTime) {
		this.depTime = depTime;
	}

	public Integer getCrsDepTime() {
		return crsDepTime;
	}

	public void setCrsDepTime(Integer crsDepTime) {
		this.crsDepTime = crsDepTime;
	}

	public Integer getArrTime() {
		return arrTime;
	}

	public void setArrTime(Integer arrTime) {
		this.arrTime = arrTime;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	public Integer getFlightNum() {
		return flightNum;
	}

	public void setFlightNum(Integer flightNum) {
		this.flightNum = flightNum;
	}

	public String getTailNum() {
		return tailNum;
	}

	public void setTailNum(String tailNum) {
		this.tailNum = tailNum;
	}

	public Integer getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(Integer actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public Integer getCrsElapsedTime() {
		return crsElapsedTime;
	}

	public void setCrsElapsedTime(Integer crsElapsedTime) {
		this.crsElapsedTime = crsElapsedTime;
	}

	public Integer getAirTime() {
		return airTime;
	}

	public void setAirTime(Integer airTime) {
		this.airTime = airTime;
	}

	public Integer getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(Integer arrDelay) {
		this.arrDelay = arrDelay;
	}

	public Integer getDepDelay() {
		return depDelay;
	}

	public void setDepDelay(Integer depDelay) {
		this.depDelay = depDelay;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public String getDistance() {
		return distance;
	}

	public void setDistance(String distance) {
		this.distance = distance;
	}

	public String getTaxiIn() {
		return TaxiIn;
	}

	public void setTaxiIn(String taxiIn) {
		TaxiIn = taxiIn;
	}

	public String getTaxiOut() {
		return TaxiOut;
	}

	public void setTaxiOut(String taxiOut) {
		TaxiOut = taxiOut;
	}

	public String getAncelled() {
		return ancelled;
	}

	public void setAncelled(String ancelled) {
		this.ancelled = ancelled;
	}

	public String getCancellationCode() {
		return cancellationCode;
	}

	public void setCancellationCode(String cancellationCode) {
		this.cancellationCode = cancellationCode;
	}

	public String getDiverted() {
		return diverted;
	}

	public void setDiverted(String diverted) {
		this.diverted = diverted;
	}

	public String getCarrierDelay() {
		return carrierDelay;
	}

	public void setCarrierDelay(String carrierDelay) {
		this.carrierDelay = carrierDelay;
	}

	public String getWeatherDelay() {
		return weatherDelay;
	}

	public void setWeatherDelay(String weatherDelay) {
		this.weatherDelay = weatherDelay;
	}

	public String getNasDelay() {
		return nasDelay;
	}

	public void setNasDelay(String nasDelay) {
		this.nasDelay = nasDelay;
	}

	public String getSecurityDelay() {
		return securityDelay;
	}

	public void setSecurityDelay(String securityDelay) {
		this.securityDelay = securityDelay;
	}

	public String getLateAircraftDelay() {
		return lateAircraftDelay;
	}

	public void setLateAircraftDelay(String lateAircraftDelay) {
		this.lateAircraftDelay = lateAircraftDelay;
	}

}

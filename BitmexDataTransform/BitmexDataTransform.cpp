// BitmexDataTransform.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <string>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <string>
#include <stdlib.h>

namespace fs = std::experimental::filesystem;

unsigned long long convert_raw_time(std::string time_raw);

struct quote_event_row
{
	unsigned long long timestamp;
	int bidsize;
	float bidprice;
	int asksize;
	float askprice;
};

struct trade_event_row
{
	unsigned long long timestamp;
	int side;
	int size;
	float price;
	//DANGER: SIZE IF 24 BYTES but we have to read in 20 bytes only
};

//2016 and 2020 are leap years, so we dont have to take leap years into account, multiplying with 365 is OK
int main()
{
	//--------------TESTING BACK TRANSFORMED DATA---------------------------------------------------------------------------------------------------------------------------
	bool testreadmode = 1;
	if (testreadmode)
	{
		std::ifstream ti;
		ti.open("C:\\EreBere\\Project\\Anaconda\\BTC_TRADE\\trade_201701.bin", std::ios::in | std::ios::binary);

		trade_event_row testin;
		size_t num_bytes_to_read = 20;

		unsigned long long one_day_in_microsecs = 86400000000ULL;
		int last_day = 0;
		int curr_day;
		int cnt = 0;

		while (!ti.eof())
		{
			ti.read(reinterpret_cast<char*>(&testin), num_bytes_to_read);
			cnt++;
			curr_day = testin.timestamp / one_day_in_microsecs;
			if (curr_day != last_day)
			{
				last_day = curr_day;
				std::cout << cnt << std::endl;
				cnt = 0;
			}
		}
		ti.close();
		return 1;
	}

	//----------ACTUAL CONVERTER PROGRAM------------------------------------------------------------------------------------------------------------------------------------

	//path to the directory where bitmex public data is
	std::string dir_path = "C:\\EreBere\\Project\\Anaconda\\BTC_TRADE\\201701";
	//variables for input handling
	std::fstream current_file;
	std::string current_row;
	//field delimiter in the rows
	std::string delimiter = ",";
	//used fields of each roaw
	std::string time_raw;
	std::string symbol_raw;
	std::string side_raw;
	std::string size_raw;
	std::string price_raw;
	std::string asksize_raw;
	//variables for splitting the row
	int next_coma;
	//variable for verbosity only
	std::string last_symbol = "erebere";
	//indicator for processing the row
	bool reached_xbtusd;

	//output file stream
	std::ofstream output_file("C:\\EreBere\\Project\\Anaconda\\BTC_TRADE\\trade_201701.bin", std::ios::out | std::ios::binary);
	if (!output_file.is_open()) { return false; }
	//fields in their output form
	unsigned long long timestamp; //at least 64 bits, 2^64 >> 10^6 * 3600 * 24 * 365 * 3 (which is 3 years in microseconds)
	int size, side;
	float price;
	//writing to binary requires a byte stream, which is implemented by interpreting variables as char
	size_t size_of_ull = sizeof(unsigned long long);
	size_t size_of_int = sizeof(int);
	size_t size_of_float = sizeof(float);
	//counting rows per day
	int cnt;

	for (const auto& entry : fs::directory_iterator(dir_path))
	{
		std::cout << "Transforming: " << entry.path() << std::endl;

		current_file.open(entry.path(), std::ios::in);
		std::getline(current_file, current_row); // we get the first line out, since it is just a header
		reached_xbtusd = false;
		cnt = 0;

		while (!current_file.eof())
		{
			std::getline(current_file, current_row);

			//to speed up things we are going to take assumptions about the data
			//1: the timestamp and symbol fields have always the same length
			//2: XBTUSD symbol makes a continuos interval inside the file !!THIS DOES NOT HOLD TRUE FOR 2018!!

			symbol_raw = current_row.substr(30, 6);

			//arrived to a different symbol
			if (symbol_raw != last_symbol)
			{
				std::cout << symbol_raw << std::endl;
				last_symbol = symbol_raw;

				if (reached_xbtusd) { break; } //break from while, finish file

				if (symbol_raw == "XBTUSD")	{ reached_xbtusd = true; }
			}

			//we are reading XBTUSD part
			if (reached_xbtusd)
			{
				//we extract time_raw, and cut the line after symbol field
				time_raw = current_row.substr(0, 29); //string (e.g. 2017-01-03D00:24:18.563095000)
				current_row = current_row.substr(37);

				next_coma = current_row.find(delimiter);
				side_raw = current_row.substr(0, next_coma);
				current_row = current_row.substr(next_coma + 1);

				next_coma = current_row.find(delimiter);
				size_raw = current_row.substr(0, next_coma); //in case of XBTUSD we dont need to worry about 1.042e-05 kind of formatting
				current_row = current_row.substr(next_coma + 1);

				next_coma = current_row.find(delimiter);
				price_raw = current_row.substr(0, next_coma); //in case of XBTUSD we dont need to worry about 1.042e-05 kind of formatting

				//now the conversion from string to numerical types
				timestamp = convert_raw_time(time_raw);		//Time in microseconds (unsigned long long)
				side = 1;
				if (side_raw == "Sell")
				{
					side = -1;
				}
				size = std::stoi(size_raw.c_str());
				price = std::stof(price_raw.c_str());

				//writing to binary file
				output_file.write(reinterpret_cast<const char*>(&timestamp), size_of_ull);
				output_file.write(reinterpret_cast<const char*>(&side), size_of_int);
				output_file.write(reinterpret_cast<const char*>(&size), size_of_int);
				output_file.write(reinterpret_cast<const char*>(&price), size_of_float);
				cnt++;
			}
		}
		std::cout << "number of XBTUSD events: " << cnt << std::endl;
		current_file.close();
	}
	output_file.close();
}

unsigned long long convert_raw_time(std::string time_raw)
{
	unsigned long long result = 0;
	result += (static_cast<unsigned long long>(std::stoi(time_raw.substr(0, 4).c_str())) - 2017ULL) * 31536000000000ULL; //365*24*3600*1000000
	int month = std::stoi(time_raw.substr(5, 2).c_str());
	switch (month)
	{
	case 1:
		break;
	case 2:
		result += 31ULL * 86400000000ULL; //24*3600*1000000
		break;
	case 3:
		result += 59ULL * 86400000000ULL;
		break;
	case 4:
		result += 90ULL * 86400000000ULL;
		break;
	case 5:
		result += 120ULL * 86400000000ULL;
		break;
	case 6:
		result += 151ULL * 86400000000ULL;
		break;
	case 7:
		result += 181ULL * 86400000000ULL;
		break;
	case 8:
		result += 212ULL * 86400000000ULL;
		break;
	case 9:
		result += 243ULL * 86400000000ULL;
		break;
	case 10:
		result += 273ULL * 86400000000ULL;
		break;
	case 11:
		result += 304ULL * 86400000000ULL;
		break;
	case 12:
		result += 334ULL * 86400000000ULL;
		break;
	default:
		break;
	}
	result += (static_cast<unsigned long long>(std::stoi(time_raw.substr(8, 2).c_str())) - 1ULL) * 86400000000ULL; //adding day
	result += static_cast<unsigned long long>(std::stoi(time_raw.substr(11, 2).c_str())) * 3600000000ULL; //adding hour
	result += static_cast<unsigned long long>(std::stoi(time_raw.substr(14, 2).c_str())) * 60000000ULL; //adding minute
	result += static_cast<unsigned long long>(std::stoi(time_raw.substr(17, 2).c_str())) * 1000000ULL; //adding seconds
	result += static_cast<unsigned long long>(std::stoi(time_raw.substr(20, 6).c_str())); //adding microseconds

	return result;
}

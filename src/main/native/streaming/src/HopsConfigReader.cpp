//============================================================================
// Name        : ConfigReader.cpp
// Created on  : Feb 9, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Configuration reading, supporting only some predefined syntax
//============================================================================

#ifndef __linux__
#error "Creating a shared library need linux OS support"
#endif

#include "../include/HopsConfigReader.h"
using namespace cnf;

HopsConfigFile::HopsConfigFile(const char* zConfigFile) {
	std::ifstream file(zConfigFile, std::ifstream::in);
	if (!file) {
		std::cout << "\033[22;33m\aConfig File not found in \033[1;31m "
				<< zConfigFile << "\033[0m" << std::endl;
		if (system("setterm -default") == -1)
			std::cout << " It wasn't possible to create the shell process"
					<< std::endl;

		char zCurrentPath[FILENAME_MAX];
		if (getcwd(zCurrentPath, sizeof(zCurrentPath)) == NULL) {
			std::cout << "Get cwd  is failed .." << std::endl;
		}
		std::cout << "CWD : " << zCurrentPath << std::endl;

		exit(EXIT_FAILURE);
	}

	std::string line, name, value, inSection;
	int posEqual;

	while (getline(file, line)) {

		if (!line.length())
			continue;

		if (line[0] == '#')
			continue;
		if (line[0] == '[') {
			inSection = Trim(line.substr(1, line.find(']') - 1));
			continue;
		}

		posEqual = line.find('=');
		name = Trim(line.substr(0, posEqual));
		value = Trim(line.substr(posEqual + 1));

		map_ConfigValues[name] = value;
	}
	file.close();
}

HopsConfigFile::~HopsConfigFile() {
	map_ConfigValues.clear();

}

std::string HopsConfigFile::Trim(std::string const& source,
		char const* delims) {
	std::string result(source);
	std::string::size_type index = result.find_last_not_of(delims);
	if (index != std::string::npos)
		result.erase(++index);

	index = result.find_first_not_of(delims);
	if (index != std::string::npos)
		result.erase(0, index);
	else
		result.erase();
	return result;
}

const char* HopsConfigFile::GetValue(const char* zTag, bool _bNoExit) {
	std::string s(zTag);
	std::map<std::string, std::string>::iterator iteConfig;
	iteConfig = map_ConfigValues.find(s);
	if (iteConfig != map_ConfigValues.end()) {
		std::string sValue = iteConfig->second;
		return sValue.data();
	} else {
		if (!_bNoExit) {
			std::cout << "\033[22;33m\aError in reading Config File\033[0m"
					<< std::endl;
			std::cout << "Requested key\033[1;31m " << s;
			std::cout << " \033[0mcould not find in the Config file"
					<< std::endl;
			if (system("setterm -default") == -1)
				std::cout << " It wasn't possible to create the shell process"
						<< std::endl;
			exit(EXIT_FAILURE);
		} else {
			return NULL;
		}
	}
}
const char* HopsConfigFile::GetValueByFormatting(const char* zFormat, ...) {
	char zBuf[1000];
	va_list ap;
	va_start(ap, zFormat);
	vsnprintf(zBuf, 999, zFormat, ap);
	va_end(ap);
	return GetValue(zBuf);
}

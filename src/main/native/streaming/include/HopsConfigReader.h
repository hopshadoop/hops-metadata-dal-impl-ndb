/*
 * ConfigReader.h
 *
 *  Created on: Feb 9, 2015
 *      Author: sri
 */

#ifndef HOPSCONFIGREADER_H_
#define HOPSCONFIGREADER_H_

#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <string.h>
using namespace std;

namespace cnf
{
			class HopsStringTokenizer
			{
			public:
				HopsStringTokenizer(char * zSource, char cDelim);
				HopsStringTokenizer();

				inline int GetCount(){
					return (int) vec_tokens.size();
				}
				char * GetTokenAt(int iIndex);
				int SplitLines(char * zSource);
			private:
				void Split(char * zSource, char cDelim);
				std::vector<char *> vec_tokens;
			};

	class HopsConfigFile
	{
	public:
		HopsConfigFile(const char* zConfigFile);
		virtual ~HopsConfigFile();

		const char* GetValue(const char* sTag, bool _bNoExit = false);
		const char* GetValueByFormatting(const char* zFormat, ...);

		std::map<std::string, std::string> map_ConfigValues;

	private:
		std::string Trim(std::string const& source, char const* delims = " \t\r\n");
	};
}

#endif /* HOPSCONFIGREADER_H_ */

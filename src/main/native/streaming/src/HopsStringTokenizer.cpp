//============================================================================
// Name        : HopsStringTokenizer.cpp
// Created on  : Feb 9, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Part of the configuration reader
//============================================================================
#include "../include/HopsConfigReader.h"
using namespace cnf;


HopsStringTokenizer::HopsStringTokenizer(char * zSource, char cDelim)
{
	Split(zSource, cDelim);
}
HopsStringTokenizer::HopsStringTokenizer()
{

}
void HopsStringTokenizer::Split(char * _zSource, char _cDelim)
{
	if (_zSource)
	{
		int iLen = strlen(_zSource);
		int iPos = 0;
		for (int i = 0; i < iLen; ++i)
		{
			if (_zSource[i] == _cDelim)
			{
				_zSource[i] = 0;
				vec_tokens.push_back(_zSource + iPos);
				iPos = i + 1;
			}
		}
		vec_tokens.push_back(_zSource + iPos);
	}
}

char * HopsStringTokenizer::GetTokenAt(int iIndex)
{
	if (iIndex < (int) vec_tokens.size())
		return vec_tokens[iIndex];
	else
		return NULL;
}

int HopsStringTokenizer::SplitLines(char * _zSource)
{
	char cDelim = '\n';
	int iProcessedLen = -1;
	if (_zSource)
	{
		int iLen = strlen(_zSource);
		int iPos = 0;
		for (int i = 0; i < iLen; ++i)
		{
			if (_zSource[i] == cDelim)
			{
				_zSource[i] = 0;
				vec_tokens.push_back(_zSource + iPos);
				iPos = i + 1;
				iProcessedLen = i;
			}
		}
	}
	return ++iProcessedLen;
}


/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/* 
 * File:   common.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <istream>
#include <ostream>
#include <string>
#include <NdbApi.hpp>
#include "vector"
#include "Logger.h"

using namespace std;

//Constansts

#define RETRIES 5
#define DELAY_BETWEEN_RETRIES 3
#define VERBOSE 0
#define WAIT_UNTIL_READY 30
#define DEFAULT_MAX_CAPACITY 10000

#endif /* COMMON_H */


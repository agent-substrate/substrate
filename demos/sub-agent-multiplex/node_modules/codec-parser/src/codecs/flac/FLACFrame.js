/* Copyright 2020-2023 Ethan Halsall
    
    This file is part of codec-parser.
    
    codec-parser is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    codec-parser is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>
*/

import { headerStore } from "../../globals.js";
import { flacCrc16 } from "../../utilities.js";
import {
  length,
  streamInfo,
  crc16,
  samples,
  subarray,
  checkFrameFooterCrc16,
} from "../../constants.js";
import CodecFrame from "../CodecFrame.js";

export default class FLACFrame extends CodecFrame {
  static _getFrameFooterCrc16(data) {
    return (data[data[length] - 2] << 8) + data[data[length] - 1];
  }

  // check frame footer crc
  // https://xiph.org/flac/format.html#frame_footer
  static [checkFrameFooterCrc16](data) {
    const expectedCrc16 = FLACFrame._getFrameFooterCrc16(data);
    const actualCrc16 = flacCrc16(data[subarray](0, -2));

    return expectedCrc16 === actualCrc16;
  }

  constructor(data, header, streamInfoValue) {
    header[streamInfo] = streamInfoValue;
    header[crc16] = FLACFrame._getFrameFooterCrc16(data);

    super(header, data, headerStore.get(header)[samples]);
  }
}

/**Proxy: Logout and clear cookies*/
import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    // Clear cookies
    const res = NextResponse.json({ success: true }, { status: 200 });

    res.cookies.delete('access_token');
    res.cookies.delete('refresh_token');

    return res;
  } catch (error) {
    return NextResponse.json({ error: 'Logout failed' }, { status: 500 });
  }
}

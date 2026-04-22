/**Proxy: Refresh access token and set new cookie*/
import { NextRequest, NextResponse } from 'next/server';
import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export async function POST(request: NextRequest) {
  try {
    const refreshToken = request.cookies.get('refresh_token')?.value;

    if (!refreshToken) {
      return NextResponse.json({ error: 'No refresh token' }, { status: 401 });
    }

    // Call backend /refresh endpoint with refresh token
    const response = await axios.post(`${API_BASE_URL}/api/v4/auth/refresh`, {}, {
      headers: {
        Authorization: `Bearer ${refreshToken}`,
      },
    });

    const { access_token } = response.data;

    // Create response
    const res = NextResponse.json({ success: true }, { status: 200 });

    // Set new access_token cookie
    res.cookies.set('access_token', access_token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 15 * 60, // 15 minutes
      path: '/',
    });

    return res;
  } catch (error: any) {
    const status = error.response?.status || 401;
    return NextResponse.json({ error: 'Token refresh failed' }, { status });
  }
}

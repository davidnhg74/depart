/**Proxy: Login and set httpOnly cookies*/
import { NextRequest, NextResponse } from 'next/server';
import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    // Call backend auth endpoint
    const response = await axios.post(`${API_BASE_URL}/api/v4/auth/login`, body);

    const { access_token, refresh_token } = response.data;

    // Create response
    const res = NextResponse.json(
      { success: true, user: response.data.user },
      { status: 200 }
    );

    // Set httpOnly cookies (secure, httpOnly, sameSite)
    res.cookies.set('access_token', access_token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 15 * 60, // 15 minutes
      path: '/',
    });

    res.cookies.set('refresh_token', refresh_token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 7 * 24 * 60 * 60, // 7 days
      path: '/',
    });

    return res;
  } catch (error: any) {
    const status = error.response?.status || 500;
    const detail = error.response?.data?.detail || 'Login failed';
    return NextResponse.json({ error: detail }, { status });
  }
}

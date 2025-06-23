#!/usr/bin/env python3
"""
Hadoop Cleaner Test Runner - Türkçe
HadoopCleanerProcessor testlerini çalıştırmak için basit script
"""

import sys
import os
import unittest

# Test dosyasının yolunu ekle
sys.path.append(os.path.dirname(__file__))

def run_tests():
    """Testleri çalıştır ve Türkçe sonuçları göster"""
    print("🚀 Hadoop Cleaner Testleri Başlatılıyor...")
    print("=" * 50)
    
    # Test dosyasını yükle
    from tests.test_hadoop_cleaner import TestHadoopCleanerProcessor
    
    # Test suite'ini oluştur
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestHadoopCleanerProcessor)
    
    # Testleri çalıştır
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Sonuçları Türkçe olarak göster
    print("\n" + "=" * 50)
    print("📊 TEST SONUÇLARI:")
    print(f"✅ Başarılı Testler: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Başarısız Testler: {len(result.failures)}")
    print(f"⚠️  Hatalı Testler: {len(result.errors)}")
    print(f"📈 Toplam Test: {result.testsRun}")
    
    if result.failures:
        print("\n❌ BAŞARISIZ TESTLER:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print("\n⚠️  HATALI TESTLER:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('Exception:')[-1].strip()}")
    
    if result.wasSuccessful():
        print("\n🎉 Tüm testler başarıyla geçti!")
        return 0
    else:
        print("\n💥 Bazı testler başarısız oldu!")
        return 1

if __name__ == '__main__':
    exit_code = run_tests()
    sys.exit(exit_code) 